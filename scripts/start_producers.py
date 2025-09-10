#!/usr/bin/env python3

import sys
import os
import time
import threading
import schedule
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producers.newsapi_producer import NewsAPIProducer
from producers.gnews_producer import GNewsProducer
from utilities.logger_config import setup_logger

load_dotenv()

class ProducerManager:
    
    def __init__(self):
        self.logger = setup_logger('ProducerManager', 'producers.log')
        self.newsapi_producer = None
        self.gnews_producer = None
        self.running = True
        
        self.interval = int(os.getenv('PRODUCER_INTERVAL_SECONDS', 300))
    
    def setup_producers(self):
        try:
            self.logger.info("Setting up producers...")
            self.newsapi_producer = NewsAPIProducer()
            self.gnews_producer = GNewsProducer()
            self.logger.info("Producers setup completed")
        except Exception as e:
            self.logger.error(f"Producer setup failed: {e}")
            raise
    
    def run_newsapi_producer(self):
        try:
            self.logger.info("Running NewsAPI producer...")
            self.newsapi_producer.process_and_send()
        except Exception as e:
            self.logger.error(f"NewsAPI producer error: {e}")
    
    def run_gnews_producer(self):
        try:
            self.logger.info("Running GNews producer...")
            self.gnews_producer.process_and_send()
        except Exception as e:
            self.logger.error(f"GNews producer error: {e}")
    
    def schedule_producers(self):
        schedule.every(self.interval).seconds.do(self.run_newsapi_producer)
        schedule.every(self.interval).seconds.do(self.run_gnews_producer)
        self.logger.info(f"Producers scheduled to run every {self.interval} seconds")
    
    def run_scheduler(self):
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def run_initial_fetch(self):
        self.logger.info("Running initial data fetch...")
        
        newsapi_thread = threading.Thread(target=self.run_newsapi_producer)
        gnews_thread = threading.Thread(target=self.run_gnews_producer)
        
        newsapi_thread.start()
        gnews_thread.start()
        
        newsapi_thread.join()
        gnews_thread.join()
        
        self.logger.info("Initial data fetch completed")
    
    def start(self):
        try:
            self.setup_producers()
            
            self.run_initial_fetch()
            
            self.schedule_producers()
            
            self.logger.info("Starting producer scheduler...")
            self.run_scheduler()
            
        except KeyboardInterrupt:
            self.logger.info("Producers stopped by user")
            self.stop()
        except Exception as e:
            self.logger.error(f"Producer manager error: {e}")
            self.stop()
    
    def stop(self):
        self.running = False
        
        if self.newsapi_producer:
            self.newsapi_producer.close()
        
        if self.gnews_producer:
            self.gnews_producer.close()
        
        self.logger.info("Producers stopped")

def main():
    manager = ProducerManager()
    manager.start()

if __name__ == "__main__":
    main()