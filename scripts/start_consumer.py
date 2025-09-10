#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumers.mongodb_consumer import MongoDBConsumer
from utilities.logger_config import setup_logger

def main():
    logger = setup_logger('ConsumerStarter', 'consumer_starter.log')
    
    try:
        logger.info("Starting MongoDB Consumer...")
        consumer = MongoDBConsumer()
        consumer.run()
        
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()