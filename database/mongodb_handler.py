from pymongo import MongoClient, errors
from pymongo.collection import Collection
from configs.mongodb_config import MongoDBConfig
from utilities.logger_config import setup_logger
from utilities.error_handler import retry_on_failure, MongoDBError
from datetime import datetime
from typing import Dict, Any, List, Optional

class MongoDBHandler:
    
    def __init__(self):
        self.config = MongoDBConfig()
        self.client = None
        self.db = None
        self.logger = setup_logger('MongoDBHandler', 'mongodb.log')
        self.connect()
    
    @retry_on_failure(max_retries=3, delay=2)
    def connect(self):
        try:
            connection_string = self.config.get_connection_string()
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000
            )
            
            self.client.admin.command('ismaster')
            self.db = self.client[self.config.DATABASE]
            
            self.logger.info("Connected to MongoDB successfully")
            
        except errors.ServerSelectionTimeoutError as e:
            self.logger.error(f"MongoDB connection timeout: {e}")
            raise MongoDBError(f"Connection timeout: {e}")
        except Exception as e:
            self.logger.error(f"MongoDB connection failed: {e}")
            raise MongoDBError(f"Connection failed: {e}")
    
    def get_collection(self, collection_name: str) -> Collection:
        if not self.db:
            raise MongoDBError("Database not connected")
        return self.db[collection_name]
    
    @retry_on_failure(max_retries=3)
    def insert_article(self, article_data: Dict[str, Any], collection_name: str) -> Optional[str]:
        try:
            collection = self.get_collection(collection_name)
            
            article_data['inserted_at'] = datetime.utcnow()
            
            article_url = article_data.get('article', {}).get('url')
            if article_url:
                existing = collection.find_one({'article.url': article_url})
                if existing:
                    self.logger.debug(f"Duplicate article skipped: {article_url}")
                    return None
            
            result = collection.insert_one(article_data)
            self.logger.debug(f"Article inserted with ID: {result.inserted_id}")
            return str(result.inserted_id)
            
        except errors.DuplicateKeyError:
            self.logger.warning("Duplicate article skipped")
            return None
        except Exception as e:
            self.logger.error(f"Failed to insert article: {e}")
            raise MongoDBError(f"Insert failed: {e}")
    
    def create_indexes(self):
        try:
            raw_collection = self.get_collection(self.config.COLLECTION_RAW)
            raw_collection.create_index([('article.url', 1)], unique=True, background=True)
            raw_collection.create_index([('article.publishedAt', -1)], background=True)
            raw_collection.create_index([('source_api', 1)], background=True)
            
            processed_collection = self.get_collection(self.config.COLLECTION_PROCESSED)
            processed_collection.create_index([('article.url', 1)], unique=True, background=True)
            processed_collection.create_index([('processed_at', -1)], background=True)
            
            self.logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {e}")
            raise MongoDBError(f"Index creation failed: {e}")
    
    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")