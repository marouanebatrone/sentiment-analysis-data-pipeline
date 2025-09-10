import os
from dotenv import load_dotenv

load_dotenv()

class APIConfig:
    NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')
    GNEWS_API_KEY = os.getenv('GNEWS_API_KEY')
    
    NEWSAPI_BASE_URL = "https://newsapi.org/v2"
    GNEWS_BASE_URL = "https://gnews.io/api/v4"
    
    DEFAULT_COUNTRY = "us"
    DEFAULT_LANGUAGE = "en"
    DEFAULT_PAGE_SIZE = 100
    
    @classmethod
    def validate_keys(cls):
        if not cls.NEWSAPI_KEY:
            raise ValueError("NEWSAPI_KEY not found!!")
        if not cls.GNEWS_API_KEY:
            raise ValueError("GNEWS_API_KEY not found!!")