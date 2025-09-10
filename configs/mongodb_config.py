import os
from dotenv import load_dotenv

load_dotenv()

class MongoDBConfig:
    HOST = os.getenv('MONGODB_HOST', 'localhost')
    PORT = int(os.getenv('MONGODB_PORT', 27017))
    USERNAME = os.getenv('MONGODB_USERNAME', 'admin')
    PASSWORD = os.getenv('MONGODB_PASSWORD', 'password123')
    DATABASE = os.getenv('MONGODB_DATABASE', 'news_db')
    COLLECTION_RAW = os.getenv('MONGODB_COLLECTION_RAW', 'raw_news')
    COLLECTION_PROCESSED = os.getenv('MONGODB_COLLECTION_PROCESSED', 'processed_news')
    
    @classmethod
    def get_connection_string(cls):
        return f"mongodb://{cls.USERNAME}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.DATABASE}"