from elasticsearch import Elasticsearch
from configs.elasticsearch_config import ElasticsearchConfig
from utilities.logger_config import setup_logger
from utilities.error_handler import retry_on_failure
from typing import Dict, Any, List, Optional
import hashlib
import json
from datetime import datetime

class ElasticsearchHandler:
    
    def __init__(self):
        self.logger = setup_logger('ElasticsearchHandler', 'elasticsearch.log')
        self.client = None
        self.config = ElasticsearchConfig()
        self.connect()
    
    @retry_on_failure(max_retries=3, delay=2)
    def connect(self):
        try:
            self.client = self.config.get_client()
            
            if self.client.ping():
                self.logger.info("Connected to Elasticsearch successfully")
            else:
                raise ConnectionError("Failed to ping Elasticsearch")
                
        except Exception as e:
            self.logger.error(f"Elasticsearch connection failed: {e}")
            raise
    
    def create_index(self, index_name: str, mapping: Dict[str, Any] = None) -> bool:
        try:
            if self.client.indices.exists(index=index_name):
                self.logger.info(f"Index {index_name} already exists")
                return True
            
            if not mapping:
                mapping = self.config.get_news_mapping()
            
            self.client.indices.create(index=index_name, body=mapping)
            self.logger.info(f"Created index: {index_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create index {index_name}: {e}")
            raise
    
    @retry_on_failure(max_retries=3)
    def index_document(self, index: str, doc_id: str, document: Dict[str, Any]) -> bool:
        try:
            response = self.client.index(
                index=index,
                id=doc_id,
                body=document
            )
            
            if response.get('result') in ['created', 'updated']:
                self.logger.debug(f"Document indexed successfully: {doc_id}")
                return True
            else:
                self.logger.warning(f"Unexpected indexing result: {response}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to index document {doc_id}: {e}")
            raise
    
    def bulk_index_documents(self, index: str, documents: List[Dict[str, Any]]) -> int:
        try:
            if not documents:
                self.logger.info("No documents to index")
                return 0
            
            bulk_data = []
            for doc in documents:
                doc_id = self.generate_document_id(doc)
                
                bulk_data.append({
                    "index": {
                        "_index": index,
                        "_id": doc_id
                    }
                })
                
                bulk_data.append(doc)
            
            response = self.client.bulk(body=bulk_data)
            
            successful = 0
            failed = 0
            
            for item in response['items']:
                if 'index' in item:
                    if item['index']['status'] in [200, 201]:
                        successful += 1
                    else:
                        failed += 1
                        self.logger.warning(f"Failed to index document: {item['index']}")
            
            self.logger.info(f"Bulk indexed {successful} documents, {failed} failed")
            return successful
            
        except Exception as e:
            self.logger.error(f"Bulk indexing failed: {e}")
            raise
    
    def search_documents(self, index: str, query: Dict[str, Any], size: int = 100) -> List[Dict[str, Any]]:
        try:
            response = self.client.search(
                index=index,
                body=query,
                size=size
            )
            
            documents = []
            for hit in response['hits']['hits']:
                doc = hit['_source']
                doc['_id'] = hit['_id']
                doc['_score'] = hit['_score']
                documents.append(doc)
            
            self.logger.info(f"Found {len(documents)} documents")
            return documents
            
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            raise
    
    def get_sentiment_aggregations(self, index: str, date_range: Optional[Dict] = None) -> Dict[str, Any]:
        try:
            query = {
                "size": 0,
                "aggs": {
                    "sentiment_distribution": {
                        "terms": {"field": "sentiment.overall"}
                    },
                    "sentiment_over_time": {
                        "date_histogram": {
                            "field": "published_at",
                            "calendar_interval": "1h"
                        },
                        "aggs": {
                            "sentiment_breakdown": {
                                "terms": {"field": "sentiment.overall"}
                            },
                            "avg_confidence": {
                                "avg": {"field": "sentiment.confidence"}
                            }
                        }
                    },
                    "source_sentiment": {
                        "terms": {"field": "source.api"},
                        "aggs": {
                            "sentiment_breakdown": {
                                "terms": {"field": "sentiment.overall"}
                            }
                        }
                    },
                    "avg_sentiment_scores": {
                        "avg": {"field": "sentiment.vader_compound"}
                    }
                }
            }
            
            if date_range:
                query["query"] = {
                    "range": {
                        "published_at": date_range
                    }
                }
            
            response = self.client.search(index=index, body=query)
            return response['aggregations']
            
        except Exception as e:
            self.logger.error(f"Aggregation query failed: {e}")
            raise
    
    def search_by_sentiment(self, index: str, sentiment: str, size: int = 50) -> List[Dict[str, Any]]:
        query = {
            "query": {
                "term": {"sentiment.overall": sentiment}
            },
            "sort": [
                {"sentiment.confidence": {"order": "desc"}},
                {"published_at": {"order": "desc"}}
            ]
        }
        
        return self.search_documents(index, query, size)
    
    def search_by_keyword(self, index: str, keyword: str, sentiment_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": keyword,
                                "fields": ["title^2", "description", "content"]
                            }
                        }
                    ]
                }
            },
            "sort": [{"_score": {"order": "desc"}}]
        }
        
        if sentiment_filter:
            query["query"]["bool"]["filter"] = [
                {"term": {"sentiment.overall": sentiment_filter}}
            ]
        
        return self.search_documents(index, query)
    
    def generate_document_id(self, document: Dict[str, Any]) -> str:
        url = document.get('url', '')
        if url:
            return hashlib.md5(url.encode()).hexdigest()
        
        content = f"{document.get('title', '')}{document.get('published_at', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def create_news_dashboard_queries(self) -> Dict[str, Dict[str, Any]]:
        return {
            "trending_positive": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"sentiment.overall": "positive"}},
                            {"range": {"published_at": {"gte": "now-24h"}}}
                        ]
                    }
                },
                "sort": [{"sentiment.confidence": {"order": "desc"}}],
                "size": 20
            },
            "trending_negative": {
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"sentiment.overall": "negative"}},
                            {"range": {"published_at": {"gte": "now-24h"}}}
                        ]
                    }
                },
                "sort": [{"sentiment.confidence": {"order": "desc"}}],
                "size": 20
            },
            "recent_articles": {
                "query": {"match_all": {}},
                "sort": [{"published_at": {"order": "desc"}}],
                "size": 50
            },
            "sentiment_summary": {
                "size": 0,
                "query": {"range": {"published_at": {"gte": "now-7d"}}},
                "aggs": {
                    "sentiment_counts": {
                        "terms": {"field": "sentiment.overall"}
                    },
                    "daily_sentiment": {
                        "date_histogram": {
                            "field": "published_at",
                            "calendar_interval": "1d"
                        },
                        "aggs": {
                            "sentiment_breakdown": {
                                "terms": {"field": "sentiment.overall"}
                            }
                        }
                    }
                }
            }
        }
    
    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("Elasticsearch connection closed")