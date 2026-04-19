# src/search/elastic_client.py
"""
Elasticsearch client wrapper for Tri-Layer Crawler.
"""

import logging
from typing import List, Dict, Optional, Iterator
from datetime import datetime

from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)


class ElasticClient:
    """Elasticsearch client with indexing and search capabilities."""
    
    INDEX_NAME = "crawled_items"
    
    # Index mapping for optimal search
    MAPPING = {
        "mappings": {
            "properties": {
                "url": {"type": "keyword"},
                "domain": {"type": "keyword"},
                "title": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword"}}
                },
                "content": {
                    "type": "text",
                    "analyzer": "standard"
                },
                "meta_description": {"type": "text"},
                "timestamp": {"type": "date"},
                "status_code": {"type": "integer"},
                "entities": {
                    "type": "nested",
                    "properties": {
                        "text": {"type": "text"},
                        "label": {"type": "keyword"},
                        "start": {"type": "integer"},
                        "end": {"type": "integer"}
                    }
                },
                "keywords": {
                    "type": "nested",
                    "properties": {
                        "keyword": {"type": "text"},
                        "score": {"type": "float"}
                    }
                },
                "sentiment_polarity": {"type": "float"},
                "sentiment_subjectivity": {"type": "float"},
                "readability_score": {"type": "float"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "analyzer": {
                    "default": {"type": "standard"}
                }
            }
        }
    }
    
    def __init__(self, host: str = "localhost", port: int = 9200):
        self.client = Elasticsearch([f"http://{host}:{port}"])
        self._ensure_index()
        logger.info(f"Elasticsearch connected to {host}:{port}")
    
    def _ensure_index(self):
        """Create index if it doesn't exist."""
        if not self.client.indices.exists(index=self.INDEX_NAME):
            self.client.indices.create(index=self.INDEX_NAME, body=self.MAPPING)
            logger.info(f"Created index: {self.INDEX_NAME}")
    
    def index_item(self, item: Dict) -> bool:
        """
        Index a single crawled item.
        
        Args:
            item: Crawled item dict with url, title, content, etc.
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            # Use URL as document ID for deduplication
            doc_id = item.get('url', '').replace('/', '_')
            
            # Prepare document
            doc = {
                **item,
                'indexed_at': datetime.utcnow().isoformat()
            }
            
            self.client.index(
                index=self.INDEX_NAME,
                id=doc_id,
                document=doc
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to index {item.get('url')}: {e}")
            return False
    
    def bulk_index(self, items: List[Dict]) -> int:
        """
        Bulk index multiple items.
        
        Args:
            items: List of crawled item dicts
            
        Returns:
            Number of successfully indexed items.
        """
        actions = []
        for item in items:
            doc_id = item.get('url', '').replace('/', '_')
            actions.append({
                "_index": self.INDEX_NAME,
                "_id": doc_id,
                "_source": {**item, 'indexed_at': datetime.utcnow().isoformat()}
            })
        
        try:
            success, failed = helpers.bulk(self.client, actions, stats_only=True)
            logger.info(f"Bulk indexed: {success} succeeded, {failed} failed")
            return success
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            return 0
    
    def search(self, query: str, size: int = 10, from_: int = 0) -> Dict:
        """
        Search crawled items.
        
        Args:
            query: Search query string
            size: Number of results
            from_: Pagination offset
            
        Returns:
            Search results with hits and metadata.
        """
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "content", "meta_description^2"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "highlight": {
                "fields": {
                    "title": {"number_of_fragments": 0},
                    "content": {"fragment_size": 150, "number_of_fragments": 3}
                }
            },
            "from": from_,
            "size": size
        }
        
        try:
            response = self.client.search(index=self.INDEX_NAME, body=search_body)
            
            hits = []
            for hit in response['hits']['hits']:
                hits.append({
                    'score': hit['_score'],
                    'url': hit['_source'].get('url'),
                    'title': hit['_source'].get('title'),
                    'content_preview': hit['_source'].get('content', '')[:200],
                    'domain': hit['_source'].get('domain'),
                    'timestamp': hit['_source'].get('timestamp'),
                    'highlight': hit.get('highlight', {})
                })
            
            return {
                'total': response['hits']['total']['value'],
                'hits': hits,
                'took_ms': response['took']
            }
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {'total': 0, 'hits': [], 'took_ms': 0, 'error': str(e)}
    
    def get_stats(self) -> Dict:
        """Get index statistics."""
        try:
            stats = self.client.indices.stats(index=self.INDEX_NAME)
            count = self.client.count(index=self.INDEX_NAME)
            
            return {
                'doc_count': count['count'],
                'size_bytes': stats['indices'][self.INDEX_NAME]['total']['store']['size_in_bytes'],
                'size_mb': round(stats['indices'][self.INDEX_NAME]['total']['store']['size_in_bytes'] / (1024*1024), 2)
            }
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {'doc_count': 0, 'size_bytes': 0, 'size_mb': 0}
    
    def delete_index(self):
        """Delete the index (use with caution)."""
        if self.client.indices.exists(index=self.INDEX_NAME):
            self.client.indices.delete(index=self.INDEX_NAME)
            logger.warning(f"Deleted index: {self.INDEX_NAME}")