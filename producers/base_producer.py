from abc import ABC, abstractmethod
from kafka import KafkaProducer
from configs.kafka_config import KafkaConfig
from utilities.logger_config import setup_logger
from utilities.error_handler import retry_on_failure, KafkaError
import json
import time

class BaseProducer(ABC):
    
    def __init__(self, topic: str):
        self.topic = topic
        self.logger = setup_logger(self.__class__.__name__, f'{self.__class__.__name__.lower()}.log')
        self.producer = None
        self.connect_kafka()
    
    def connect_kafka(self):
        try:
            self.producer = KafkaProducer(**KafkaConfig.PRODUCER_CONFIG)
            self.logger.info("Kafka producer connected successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise KafkaError(f"Kafka connection failed: {e}")
    
    @retry_on_failure(max_retries=3)
    def send_message(self, message: dict, key: str = None):
        try:
            future = self.producer.send(self.topic, value=message, key=key)
            future.get(timeout=10) 
            self.logger.debug(f"Message sent to topic {self.topic}")
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            raise KafkaError(f"Failed to send message: {e}")
    
    @abstractmethod
    def fetch_data(self):
        pass
    
    @abstractmethod
    def process_and_send(self):
        pass
    
    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed")