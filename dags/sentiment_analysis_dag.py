from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pymongo
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import logging
import hashlib

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'news_sentiment_analysis',
    default_args=default_args,
    description='Simple sentiment analysis pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['news', 'sentiment']
)

def get_connections():
    mongo_client = pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/news_db")
    es_client = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
    return mongo_client, es_client

def extract_unprocessed_articles(**context):
    mongo_client, _ = get_connections()
    db = mongo_client.news_db
    
    try:
        processed_urls = {doc['article']['url'] 
                         for doc in db.processed_news.find({}, {'article.url': 1})
                         if 'article' in doc and 'url' in doc['article']}
        
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        new_articles = list(db.raw_news.find({
            'fetched_at': {'$gte': one_hour_ago.isoformat()}
        }))
        
        unprocessed = [article for article in new_articles 
                      if article.get('article', {}).get('url') not in processed_urls]
        
        logging.info(f"Found {len(unprocessed)} unprocessed articles")
        return unprocessed
        
    finally:
        mongo_client.close()

def analyze_sentiment(**context):
    articles = context['ti'].xcom_pull(task_ids='extract_articles')
    if not articles:
        return []
    
    analyzer = SentimentIntensityAnalyzer()
    processed_articles = []
    
    for article_doc in articles:
        article = article_doc.get('article', {})
        
        text = f"{article.get('title', '')} {article.get('description', '')}"
        if article.get('content'):
            text += f" {article['content'][:500]}" 
        
        if not text.strip():
            continue
            
        vader_scores = analyzer.polarity_scores(text)
        textblob_score = TextBlob(text).sentiment.polarity
        
        avg_score = (vader_scores['compound'] + textblob_score) / 2
        if avg_score >= 0.1:
            sentiment = 'positive'
        elif avg_score <= -0.1:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        processed_doc = {
            '_id': article_doc['_id'],
            'source_api': article_doc.get('source_api'),
            'fetched_at': article_doc.get('fetched_at'),
            'processed_at': datetime.utcnow().isoformat(),
            'article': article,
            'sentiment': {
                'overall': sentiment,
                'confidence': abs(avg_score),
                'vader_compound': vader_scores['compound'],
                'textblob_polarity': textblob_score
            }
        }
        processed_articles.append(processed_doc)
    
    logging.info(f"Processed {len(processed_articles)} articles")
    return processed_articles

def save_results(**context):
    processed_articles = context['ti'].xcom_pull(task_ids='analyze_sentiment')
    if not processed_articles:
        return
    
    mongo_client, es_client = get_connections()
    
    saved_count = 0
    for article in processed_articles:
        existing = mongo_client.news_db.processed_news.find_one({
            'article.url': article['article']['url']
        })
        if not existing:
            mongo_client.news_db.processed_news.insert_one(article)
            saved_count += 1
        
        # Index to Elasticsearch
        doc_id = hashlib.md5(article['article']['url'].encode()).hexdigest()
        es_doc = {
            'title': article['article'].get('title', ''),
            'content': article['article'].get('content', '')[:1000],
            'url': article['article'].get('url', ''),
            'published_at': article['article'].get('publishedAt', ''),
            'sentiment': article['sentiment'],
            'timestamp': datetime.utcnow()
        }
        es_client.index(index='news-articles', id=doc_id, body=es_doc)
    
    logging.info(f"Saved {saved_count} articles")
    mongo_client.close()

extract_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_unprocessed_articles,
    dag=dag,
)

sentiment_task = PythonOperator(
    task_id='analyze_sentiment',
    python_callable=analyze_sentiment,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_results',
    python_callable=save_results,
    dag=dag,
)

extract_task >> sentiment_task >> save_task