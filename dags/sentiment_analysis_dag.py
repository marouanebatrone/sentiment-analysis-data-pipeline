from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
import pymongo
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
import logging
import json
from typing import List, Dict, Any
import hashlib

default_args = {
    'owner': 'news-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'news_sentiment_analysis',
    default_args=default_args,
    description='Process news articles with sentiment analysis and index to Elasticsearch',
    schedule_interval=timedelta(hours=1), 
    catchup=False,
    max_active_runs=1,
    tags=['news', 'sentiment', 'elasticsearch']
)

class MongoDBNewArticlesSensor(BaseSensorOperator):    
    def __init__(self, collection_name: str, **kwargs):
        super().__init__(**kwargs)
        self.collection_name = collection_name
        
    def poke(self, context: Context) -> bool:
        try:
            client = pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/news_db")
            db = client.news_db
            collection = db[self.collection_name]
            
            last_run = context.get('ti').xcom_pull(key='last_processed_time')
            if not last_run:
                last_run = datetime.utcnow() - timedelta(hours=1)
            else:
                last_run = datetime.fromisoformat(last_run)
            
            new_articles_count = collection.count_documents({
                'fetched_at': {'$gt': last_run.isoformat()}
            })
            
            self.log.info(f"Found {new_articles_count} new articles since {last_run}")
            
            client.close()
            return new_articles_count > 0
            
        except Exception as e:
            self.log.error(f"Error checking for new articles: {e}")
            return False

def extract_articles_from_mongodb(**context) -> List[Dict[str, Any]]:
    try:
        client = pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/news_db")
        db = client.news_db
        raw_collection = db.raw_news
        processed_collection = db.processed_news
        
        last_run = context.get('ti').xcom_pull(key='last_processed_time')
        if not last_run:
            last_run = datetime.utcnow() - timedelta(hours=1)
        else:
            last_run = datetime.fromisoformat(last_run)
        
        query = {'fetched_at': {'$gt': last_run.isoformat()}}
        new_articles = list(raw_collection.find(query))
        
        processed_urls = set()
        for article in processed_collection.find({}, {'article.url': 1}):
            if 'article' in article and 'url' in article['article']:
                processed_urls.add(article['article']['url'])
        
        unprocessed_articles = []
        for article in new_articles:
            if article.get('article', {}).get('url') not in processed_urls:
                unprocessed_articles.append(article)
        
        logging.info(f"Found {len(unprocessed_articles)} unprocessed articles")
        
        client.close()
        return unprocessed_articles
        
    except Exception as e:
        logging.error(f"Error extracting articles: {e}")
        raise

def perform_sentiment_analysis(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        vader_analyzer = SentimentIntensityAnalyzer()
        processed_articles = []
        
        for article_doc in articles:
            article = article_doc.get('article', {})
            
            text_to_analyze = ""
            if article.get('title'):
                text_to_analyze += article['title'] + " "
            if article.get('description'):
                text_to_analyze += article['description'] + " "
            if article.get('content'):
                content = article['content'][:1000]
                text_to_analyze += content
            
            if not text_to_analyze.strip():
                continue
            
            vader_scores = vader_analyzer.polarity_scores(text_to_analyze)
            
            blob = TextBlob(text_to_analyze)
            textblob_polarity = blob.sentiment.polarity
            textblob_subjectivity = blob.sentiment.subjectivity
            
            sentiment_data = {
                'vader': {
                    'compound': vader_scores['compound'],
                    'positive': vader_scores['pos'],
                    'negative': vader_scores['neg'],
                    'neutral': vader_scores['neu']
                },
                'textblob': {
                    'polarity': textblob_polarity,
                    'subjectivity': textblob_subjectivity
                },
                'overall_sentiment': classify_sentiment(vader_scores['compound'], textblob_polarity),
                'confidence_score': calculate_confidence(vader_scores, textblob_polarity)
            }
            
            processed_article = {
                '_id': article_doc['_id'],
                'source_api': article_doc.get('source_api'),
                'fetched_at': article_doc.get('fetched_at'),
                'processed_at': datetime.utcnow().isoformat(),
                'article': article,
                'sentiment': sentiment_data,
                'content_stats': {
                    'word_count': len(text_to_analyze.split()),
                    'char_count': len(text_to_analyze),
                    'has_content': bool(article.get('content'))
                }
            }
            
            processed_articles.append(processed_article)
        
        logging.info(f"Processed sentiment for {len(processed_articles)} articles")
        return processed_articles
        
    except Exception as e:
        logging.error(f"Error in sentiment analysis: {e}")
        raise

def classify_sentiment(vader_compound: float, textblob_polarity: float) -> str:
    avg_score = (vader_compound + textblob_polarity) / 2
    
    if avg_score >= 0.1:
        return 'positive'
    elif avg_score <= -0.1:
        return 'negative'
    else:
        return 'neutral'

def calculate_confidence(vader_scores: Dict, textblob_polarity: float) -> float:
    vader_confidence = abs(vader_scores['compound'])
    textblob_confidence = abs(textblob_polarity)
    return (vader_confidence + textblob_confidence) / 2

def save_to_mongodb_and_elasticsearch(**context):
    try:
        processed_articles = context['ti'].xcom_pull(task_ids='sentiment_analysis')
        
        if not processed_articles:
            logging.info("No processed articles to save")
            return
        
        mongo_client = pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/news_db")
        db = mongo_client.news_db
        processed_collection = db.processed_news
        
        es_client = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
        
        saved_count = 0
        indexed_count = 0
        
        for article in processed_articles:
            try:
                result = processed_collection.insert_one(article)
                saved_count += 1
                
                es_doc = prepare_elasticsearch_document(article)
                
                doc_id = generate_document_id(article)
                es_client.index(
                    index='news-articles',
                    id=doc_id,
                    body=es_doc
                )
                indexed_count += 1
                
            except pymongo.errors.DuplicateKeyError:
                logging.warning(f"Duplicate article skipped: {article.get('article', {}).get('url', 'Unknown')}")
            except Exception as e:
                logging.error(f"Error processing individual article: {e}")
        
        context['ti'].xcom_push(key='last_processed_time', value=datetime.utcnow().isoformat())
        
        logging.info(f"Saved {saved_count} articles to MongoDB and indexed {indexed_count} to Elasticsearch")
        
        mongo_client.close()
        
    except Exception as e:
        logging.error(f"Error saving processed articles: {e}")
        raise

def prepare_elasticsearch_document(article: Dict[str, Any]) -> Dict[str, Any]:
    article_data = article.get('article', {})
    sentiment_data = article.get('sentiment', {})
    
    return {
        'title': article_data.get('title', ''),
        'description': article_data.get('description', ''),
        'content': article_data.get('content', '')[:5000], 
        'url': article_data.get('url', ''),
        'source': {
            'api': article.get('source_api', ''),
            'name': article_data.get('source', {}).get('name', ''),
            'url': article_data.get('source', {}).get('url', '')
        },
        'author': article_data.get('author', ''),
        'published_at': article_data.get('publishedAt', ''),
        'fetched_at': article.get('fetched_at', ''),
        'processed_at': article.get('processed_at', ''),
        'sentiment': {
            'overall': sentiment_data.get('overall_sentiment', 'neutral'),
            'confidence': sentiment_data.get('confidence_score', 0.0),
            'vader_compound': sentiment_data.get('vader', {}).get('compound', 0.0),
            'textblob_polarity': sentiment_data.get('textblob', {}).get('polarity', 0.0)
        },
        'stats': article.get('content_stats', {}),
        'timestamp': datetime.utcnow()
    }

def generate_document_id(article: Dict[str, Any]) -> str:
    url = article.get('article', {}).get('url', '')
    if url:
        return hashlib.md5(url.encode()).hexdigest()
    else:
        return str(article.get('_id', ''))

def create_elasticsearch_index(**context):
    try:
        es_client = Elasticsearch([{'host': 'elasticsearch', 'port': 9200}])
        
        index_name = 'news-articles'
        
        if es_client.indices.exists(index=index_name):
            logging.info(f"Index {index_name} already exists")
            return
        
        mapping = {
            "mappings": {
                "properties": {
                    "title": {"type": "text", "analyzer": "english"},
                    "description": {"type": "text", "analyzer": "english"},
                    "content": {"type": "text", "analyzer": "english"},
                    "url": {"type": "keyword"},
                    "source": {
                        "properties": {
                            "api": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "url": {"type": "keyword"}
                        }
                    },
                    "author": {"type": "keyword"},
                    "published_at": {"type": "date"},
                    "fetched_at": {"type": "date"},
                    "processed_at": {"type": "date"},
                    "sentiment": {
                        "properties": {
                            "overall": {"type": "keyword"},
                            "confidence": {"type": "float"},
                            "vader_compound": {"type": "float"},
                            "textblob_polarity": {"type": "float"}
                        }
                    },
                    "stats": {
                        "properties": {
                            "word_count": {"type": "integer"},
                            "char_count": {"type": "integer"},
                            "has_content": {"type": "boolean"}
                        }
                    },
                    "timestamp": {"type": "date"}
                }
            }
        }
        
        es_client.indices.create(index=index_name, body=mapping)
        logging.info(f"Created Elasticsearch index: {index_name}")
        
    except Exception as e:
        logging.error(f"Error creating Elasticsearch index: {e}")
        raise

def extract_task(**context):
    return extract_articles_from_mongodb(**context)

def sentiment_task(**context):
    articles = context['ti'].xcom_pull(task_ids='extract_articles')
    return perform_sentiment_analysis(articles)

create_index_task = PythonOperator(
    task_id='create_elasticsearch_index',
    python_callable=create_elasticsearch_index,
    dag=dag,
)

check_new_articles = MongoDBNewArticlesSensor(
    task_id='check_new_articles',
    collection_name='raw_news',
    poke_interval=60, 
    timeout=300,       # 5 minutes
    dag=dag,
)

extract_articles_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_task,
    dag=dag,
)

sentiment_analysis_task = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=sentiment_task,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_to_mongodb_and_elasticsearch',
    python_callable=save_to_mongodb_and_elasticsearch,
    dag=dag,
)

create_index_task >> check_new_articles >> extract_articles_task >> sentiment_analysis_task >> save_data_task