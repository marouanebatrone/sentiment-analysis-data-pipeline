from datetime import datetime
from typing import Dict, Any, List
import re

class DataValidator:
    
    @staticmethod
    def validate_newsapi_article(article: Dict[str, Any]) -> bool:
        required_fields = ['title', 'url', 'publishedAt']
        
        for field in required_fields:
            if field not in article or not article[field]:
                return False
        
        url_pattern = r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?'
        if not re.match(url_pattern, article['url']):
            return False
            
        return True
    
    @staticmethod
    def validate_gnews_article(article: Dict[str, Any]) -> bool:
        required_fields = ['title', 'url', 'publishedAt']
        
        for field in required_fields:
            if field not in article or not article[field]:
                return False
                
        url_pattern = r'https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?'
        if not re.match(url_pattern, article['url']):
            return False
            
        return True
    
    @staticmethod
    def sanitize_article(article: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}
        
        for key, value in article.items():
            if isinstance(value, str):
                sanitized[key] = value.replace('\x00', '').strip()
            else:
                sanitized[key] = value
        
        return sanitized