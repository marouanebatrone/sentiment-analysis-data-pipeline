import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from typing import Dict, Any, List
import logging

load_dotenv()

class ElasticsearchConfig:
    HOST = os.getenv('ELASTICSEARCH_HOST', 'localhost')
    PORT = int(os.getenv('ELASTICSEARCH_PORT', 9200))
    USERNAME = os.getenv('ELASTICSEARCH_USERNAME', '')
    PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', '')
    
    NEWS_INDEX = 'news-articles'
    SENTIMENT_INDEX = 'sentiment-analysis'
    
    @classmethod
    def get_client(cls) -> Elasticsearch:
        config = {
            'hosts': [{'host': cls.HOST, 'port': cls.PORT}],
            'verify_certs': False,
            'ssl_show_warn': False
        }
        
        if cls.USERNAME and cls.PASSWORD:
            config['http_auth'] = (cls.USERNAME, cls.PASSWORD)
        
        return Elasticsearch(**config)
    
    @classmethod
    def get_news_mapping(cls) -> Dict[str, Any]:
        return {
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text", 
                        "analyzer": "english",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "description": {
                        "type": "text", 
                        "analyzer": "english"
                    },
                    "content": {
                        "type": "text", 
                        "analyzer": "english"
                    },
                    "url": {"type": "keyword"},
                    "source": {
                        "properties": {
                            "api": {"type": "keyword"},
                            "name": {"type": "keyword"},
                            "url": {"type": "keyword"}
                        }
                    },
                    "author": {"type": "keyword"},
                    "published_at": {"type": "date"},
                    "fetched_at": {"type": "date"},
                    "processed_at": {"type": "date"},
                    "sentiment": {
                        "properties": {
                            "overall": {"type": "keyword"},
                            "confidence": {"type": "float"},
                            "vader_compound": {"type": "float"},
                            "textblob_polarity": {"type": "float"},
                            "vader_details": {
                                "properties": {
                                    "positive": {"type": "float"},
                                    "negative": {"type": "float"},
                                    "neutral": {"type": "float"}
                                }
                            }
                        }
                    },
                    "stats": {
                        "properties": {
                            "word_count": {"type": "integer"},
                            "char_count": {"type": "integer"},
                            "has_content": {"type": "boolean"}
                        }
                    },
                    "categories": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "timestamp": {"type": "date"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "custom_english": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "english_stemmer", "english_stop"]
                        }
                    },
                    "filter": {
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english"
                        },
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
                        }
                    }
                }
            }
        }
    
    @classmethod
    def get_index_template(cls) -> Dict[str, Any]:
        return {
            "index_patterns": ["news-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        }