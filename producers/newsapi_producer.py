import requests
import time
from datetime import datetime
from .base_producer import BaseProducer
from configs.api_config import APIConfig
from configs.kafka_config import KafkaConfig
from utilities.data_validator import DataValidator
from utilities.error_handler import retry_on_failure, APIError

class NewsAPIProducer(BaseProducer):
    
    def __init__(self):
        super().__init__(KafkaConfig.TOPIC_NEWS_RAW)
        self.api_config = APIConfig()
        self.api_config.validate_keys()
    
    @retry_on_failure(max_retries=3, delay=2)
    def fetch_data(self, category='general', page_size=100):
        url = f"{APIConfig.NEWSAPI_BASE_URL}/top-headlines"
        
        params = {
            'apiKey': APIConfig.NEWSAPI_KEY,
            'country': APIConfig.DEFAULT_COUNTRY,
            'category': category,
            'pageSize': page_size
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'ok':
                raise APIError(f"NewsAPI error: {data.get('message', 'Unknown error')}")
            
            self.logger.info(f"Fetched {len(data.get('articles', []))} articles from NewsAPI")
            return data.get('articles', [])
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise APIError(f"NewsAPI request failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise APIError(f"NewsAPI fetch error: {e}")
    
    def process_and_send(self):
        try:
            articles = self.fetch_data()
            
            processed_count = 0
            for article in articles:
                if not DataValidator.validate_newsapi_article(article):
                    self.logger.warning("Invalid article skipped")
                    continue
                
                clean_article = DataValidator.sanitize_article(article)
                
                message = {
                    'source_api': 'newsapi',
                    'fetched_at': datetime.utcnow().isoformat(),
                    'article': clean_article
                }
                
                key = f"newsapi_{clean_article.get('url', '')}"
                self.send_message(message, key)
                processed_count += 1
            
            self.logger.info(f"Successfully processed and sent {processed_count} articles")
            
        except Exception as e:
            self.logger.error(f"Error in process_and_send: {e}")
            raise