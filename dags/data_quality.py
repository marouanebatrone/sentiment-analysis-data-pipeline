from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pymongo
import logging

default_args = {
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_quality',
    default_args=default_args,
    description='data quality monitoring',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    tags=['monitoring']
)

def get_mongo_client():
    return pymongo.MongoClient("mongodb://admin:password123@mongodb:27017/news_db")

def check_data_freshness(**context):
    client = get_mongo_client()
    
    try:
        two_hours_ago = datetime.utcnow() - timedelta(hours=2)
        recent_count = client.news_db.raw_news.count_documents({
            'fetched_at': {'$gte': two_hours_ago.isoformat()}
        })
        
        if recent_count == 0:
            logging.error("No new articles in last 2 hours!")
            raise ValueError("Data ingestion stopped!")
        
        logging.info(f"Found {recent_count} recent articles")
        return recent_count
        
    finally:
        client.close()

def check_data_quality(**context):
    client = get_mongo_client()
    
    try:
        yesterday = datetime.utcnow() - timedelta(days=1)
        
        total = client.news_db.processed_news.count_documents({
            'processed_at': {'$gte': yesterday.isoformat()}
        })
        
        if total == 0:
            logging.warning("No processed articles found")
            return 0
        
        missing_sentiment = client.news_db.processed_news.count_documents({
            'processed_at': {'$gte': yesterday.isoformat()},
            'sentiment': None
        })
        
        quality_rate = ((total - missing_sentiment) / total) * 100
        
        if quality_rate < 90:
            logging.error(f"Low quality rate: {quality_rate:.1f}%")
            raise ValueError(f"Quality issue: {missing_sentiment}/{total} missing sentiment")
        
        logging.info(f"Quality check passed: {quality_rate:.1f}% ({total} articles)")
        return total
        
    finally:
        client.close()

freshness_task = PythonOperator(
    task_id='check_data_freshness',
    python_callable=check_data_freshness,
    dag=dag,
)

quality_task = PythonOperator(
    task_id='check_data_quality', 
    python_callable=check_data_quality,
    dag=dag,
)

freshness_task >> quality_task