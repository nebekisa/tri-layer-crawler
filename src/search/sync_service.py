"""
Sync service to replicate PostgreSQL data to Elasticsearch.
"""

import logging
import time
import psycopg2
from typing import List, Dict
from datetime import datetime, timedelta

# ? ADD THIS IMPORT
from src.search.elastic_client import ElasticClient

logger = logging.getLogger(__name__)


class SyncService:
    """Sync PostgreSQL crawled items to Elasticsearch."""
    
    def __init__(self, batch_size: int = 100):
        self.es_client = ElasticClient()  # Now defined!
        self.batch_size = batch_size
        
        # Direct connection for Windows execution
        self.conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='tri_layer_crawler',
            user='crawler_user',
            password='CrawlerPass2024!'
        )
