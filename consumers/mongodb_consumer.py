from kafka import KafkaConsumer
import json
import signal
import sys
from configs.kafka_config import KafkaConfig
from configs.mongodb_config import MongoDBConfig
from database.mongodb_handler import MongoDBHandler
from utilities.logger_config import setup_logger
from utilities.error_handler import retry_on_failure, KafkaError, MongoDBError

class MongoDBConsumer:
    def __init__(self):
        self.logger = setup_logger('MongoDBConsumer', 'consumer.log')
        self.consumer = None
        self.mongodb_handler = None
        self.running = True
        
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)
    
    def setup(self):
        try:
            self.mongodb_handler = MongoDBHandler()
            self.mongodb_handler.create_indexes()
            
            self.consumer = KafkaConsumer(
                KafkaConfig.TOPIC_NEWS_RAW,
                **KafkaConfig.CONSUMER_CONFIG
            )
            
            self.logger.info("Consumer setup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Consumer setup failed: {e}")
            raise
    
    @retry_on_failure(max_retries=3)
    def process_message(self, message):
        try:
            data = message.value
            
            if not isinstance(data, dict):
                self.logger.warning("Invalid message format received")
                return
            
            if 'source_api' not in data or 'article' not in data:
                self.logger.warning("Message missing required fields")
                return
            
            result = self.mongodb_handler.insert_article(
                data, 
                MongoDBConfig.COLLECTION_RAW
            )
            
            if result:
                self.logger.debug(f"Message processed successfully from {data.get('source_api')}")
            
        except Exception as e:
            self.logger.error(f"Failed to process message: {e}")
            raise
    
    def run(self):
        self.setup()
        
        self.logger.info("Starting MongoDB consumer...")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    self.process_message(message)
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
        finally:
            self.cleanup()
    
    def shutdown(self, signum, frame):
        self.logger.info("Shutdown signal received, stopping consumer...")
        self.running = False
    
    def cleanup(self):
        if self.consumer:
            self.consumer.close()
            self.logger.info("Kafka consumer closed")
        
        if self.mongodb_handler:
            self.mongodb_handler.close()
            self.logger.info("MongoDB connection closed")

if __name__ == "__main__":
    consumer = MongoDBConsumer()
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.logger.info("Consumer stopped")
    except Exception as e:
        consumer.logger.error(f"Consumer failed: {e}")
        sys.exit(1)