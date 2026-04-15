"""
Database query optimization utilities.
"""

import logging
from typing import List, Dict, Any
from sqlalchemy import text, Index
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class QueryOptimizer:
    """Optimize database queries."""
    
    @staticmethod
    def create_indexes(session: Session) -> None:
        """Create performance indexes."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_crawled_items_domain ON crawled_items(domain);",
            "CREATE INDEX IF NOT EXISTS idx_crawled_items_crawled_at ON crawled_items(crawled_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_analysis_sentiment ON analysis_results(sentiment_label);",
            "CREATE INDEX IF NOT EXISTS idx_entities_type ON extracted_entities(entity_type);",
        ]
        
        for idx_sql in indexes:
            try:
                session.execute(text(idx_sql))
                session.commit()
                logger.info(f"Created index: {idx_sql[:50]}...")
            except Exception as e:
                logger.warning(f"Index creation skipped: {e}")
    
    @staticmethod
    def analyze_query_performance(session: Session) -> Dict[str, Any]:
        """Analyze query performance (PostgreSQL)."""
        try:
            result = session.execute(text(
                "SELECT relname, seq_scan, idx_scan, n_live_tup "
                "FROM pg_stat_user_tables "
                "ORDER BY seq_scan DESC;"
            ))
            
            tables = []
            for row in result:
                tables.append({
                    'table': row[0],
                    'seq_scans': row[1],
                    'idx_scans': row[2],
                    'live_rows': row[3],
                    'idx_usage_ratio': row[2] / (row[1] + 1)
                })
            
            return {'tables': tables}
            
        except Exception as e:
            logger.warning(f"Performance analysis not available: {e}")
            return {'error': str(e)}