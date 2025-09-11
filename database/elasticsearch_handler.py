from elasticsearch import Elasticsearch
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional

class ElasticsearchHandler:
    
    def __init__(self):
        self.client = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    
    def create_index(self, index_name: str):
        if self.client.indices.exists(index=index_name):
            return True
            
        mapping = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "content": {"type": "text"},
                    "url": {"type": "keyword"},
                    "published_at": {"type": "date"},
                    "sentiment": {
                        "properties": {
                            "overall": {"type": "keyword"},
                            "confidence": {"type": "float"}
                        }
                    }
                }
            }
        }
        self.client.indices.create(index=index_name, body=mapping)
        return True
    
    def index_document(self, index: str, doc_id: str, document: Dict[str, Any]):
        self.client.index(index=index, id=doc_id, body=document)
    
    def bulk_index(self, index: str, documents: List[Dict[str, Any]]):
        if not documents:
            return 0
            
        bulk_data = []
        for doc in documents:
            doc_id = self.generate_doc_id(doc)
            bulk_data.append({"index": {"_index": index, "_id": doc_id}})
            bulk_data.append(doc)
        
        response = self.client.bulk(body=bulk_data)
        
        successful = sum(1 for item in response['items'] 
                        if 'index' in item and item['index']['status'] in [200, 201])
        return successful
    
    def search(self, index: str, query: Dict[str, Any], size: int = 100):
        response = self.client.search(index=index, body=query, size=size)
        
        documents = []
        for hit in response['hits']['hits']:
            doc = hit['_source']
            doc['_id'] = hit['_id']
            documents.append(doc)
        
        return documents
    
    def search_by_sentiment(self, index: str, sentiment: str, size: int = 50):
        query = {
            "query": {"term": {"sentiment.overall": sentiment}},
            "sort": [{"published_at": {"order": "desc"}}]
        }
        return self.search(index, query, size)
    
    def search_by_keyword(self, index: str, keyword: str, size: int = 50):
        query = {
            "query": {
                "multi_match": {
                    "query": keyword,
                    "fields": ["title", "content"]
                }
            }
        }
        return self.search(index, query, size)
    
    def get_sentiment_stats(self, index: str):
        query = {
            "size": 0,
            "aggs": {
                "sentiment_counts": {
                    "terms": {"field": "sentiment.overall"}
                }
            }
        }
        
        response = self.client.search(index=index, body=query)
        return response['aggregations']['sentiment_counts']['buckets']
    
    def generate_doc_id(self, document: Dict[str, Any]) -> str:
        url = document.get('url', '')
        if url:
            return hashlib.md5(url.encode()).hexdigest()
        
        title = document.get('title', '')
        return hashlib.md5(title.encode()).hexdigest()
    
    def close(self):
        self.client.close()