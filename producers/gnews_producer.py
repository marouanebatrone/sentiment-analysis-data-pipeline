import requests
import time
from datetime import datetime
from .base_producer import BaseProducer
from configs.api_config import APIConfig
from configs.kafka_config import KafkaConfig
from utilities.data_validator import DataValidator
from utilities.error_handler import retry_on_failure, APIError

class GNewsProducer(BaseProducer):
    
    def __init__(self):
        super().__init__(KafkaConfig.TOPIC_NEWS_RAW)
        self.api_config = APIConfig()
        self.api_config.validate_keys()
    
    @retry_on_failure(max_retries=3, delay=2)
    def fetch_data(self, category='general', max_articles=100):
        url = f"{APIConfig.GNEWS_BASE_URL}/top-headlines"
        
        params = {
            'token': APIConfig.GNEWS_API_KEY,
            'lang': APIConfig.DEFAULT_LANGUAGE,
            'country': APIConfig.DEFAULT_COUNTRY,
            'category': category,
            'max': max_articles
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'articles' not in data:
                raise APIError(f"GNews API error: Invalid response format")
            
            self.logger.info(f"Fetched {len(data.get('articles', []))} articles from GNews")
            return data.get('articles', [])
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise APIError(f"GNews request failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise APIError(f"GNews fetch error: {e}")
    
    def process_and_send(self):
        try:
            articles = self.fetch_data()
            
            processed_count = 0
            for article in articles:
                if not DataValidator.validate_gnews_article(article):
                    self.logger.warning("Invalid article skipped")
                    continue
                
                clean_article = DataValidator.sanitize_article(article)
                
                message = {
                    'source_api': 'gnews',
                    'fetched_at': datetime.utcnow().isoformat(),
                    'article': clean_article
                }
                
                key = f"gnews_{clean_article.get('url', '')}"
                self.send_message(message, key)
                processed_count += 1
            
            self.logger.info(f"Successfully processed and sent {processed_count} articles")
            
        except Exception as e:
            self.logger.error(f"Error in process_and_send: {e}")
            raise