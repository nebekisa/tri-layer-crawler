"""
Database repository for saving crawled items.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseRepository:
    """Repository for database operations."""
    
    def __init__(self):
        self.conn = psycopg2.connect(
            host='localhost',
            port=5433,
            database='tri_layer_crawler',
            user='crawler_user',
            password='CrawlerPass2024!'
        )
        self.conn.autocommit = False
        logger.info("DatabaseRepository connected")
    
    def save_item(self, item: Dict) -> Optional[int]:
        """Save a single crawled item."""
        try:
            cur = self.conn.cursor()
            
            # Check if URL already exists
            cur.execute(
                "SELECT id FROM crawled_items WHERE url = %s",
                (item.get('url'),)
            )
            existing = cur.fetchone()
            
            if existing:
                # Update existing
                cur.execute('''
                    UPDATE crawled_items 
                    SET title = %s, content = %s, meta_description = %s,
                        status_code = %s, content_length = %s, crawled_at = NOW()
                    WHERE id = %s
                    RETURNING id
                ''', (
                    item.get('title'),
                    item.get('content'),
                    item.get('meta_description'),
                    item.get('status_code', 200),
                    len(item.get('content', '')),
                    existing[0]
                ))
                item_id = cur.fetchone()[0]
            else:
                # Insert new
                cur.execute('''
                    INSERT INTO crawled_items 
                    (url, title, content, meta_description, domain, status_code, content_length, crawled_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    RETURNING id
                ''', (
                    item.get('url'),
                    item.get('title'),
                    item.get('content'),
                    item.get('meta_description'),
                    item.get('domain'),
                    item.get('status_code', 200),
                    len(item.get('content', ''))
                ))
                item_id = cur.fetchone()[0]
            
            self.conn.commit()
            cur.close()
            return item_id
            
        except Exception as e:
            logger.error(f"[FAIL] Database save failed for {item.get('url')}: {e}")
            self.conn.rollback()
            return None
    
    def save_batch(self, items: List[Dict]) -> int:
        """Save multiple items."""
        saved = 0
        for item in items:
            item_id = self.save_item(item)
            if item_id:
                saved += 1
        
        logger.info(f"[OK] Saved {saved}/{len(items)} items to database")
        return saved
    
    def get_items(self, limit: int = 100) -> List[Dict]:
        """Get recent items."""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute('''
            SELECT * FROM crawled_items 
            ORDER BY crawled_at DESC 
            LIMIT %s
        ''', (limit,))
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    
    def close(self):
        """Close connection."""
        if self.conn:
            self.conn.close()
            logger.debug("Database connection closed")
