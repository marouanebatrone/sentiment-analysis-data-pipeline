import os
from dotenv import load_dotenv

load_dotenv()

class KafkaConfig:
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    TOPIC_NEWS_RAW = os.getenv('KAFKA_TOPICS_NEWS_RAW', 'news-raw')
    TOPIC_NEWS_PROCESSED = os.getenv('KAFKA_TOPICS_NEWS_PROCESSED', 'news-processed')
    
    PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
        'key_serializer': lambda x: x.encode('utf-8') if x else None,
        'retries': 3,
        'acks': 1
    }
    
    CONSUMER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'group_id': 'news-consumer-group'
    }