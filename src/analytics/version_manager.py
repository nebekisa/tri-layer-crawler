"""
Data versioning and historical tracking for crawled content.
"""

import hashlib
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from difflib import unified_diff
import json

logger = logging.getLogger(__name__)


class VersionManager:
    """Manage historical versions of crawled items."""
    
    def __init__(self):
        pass
    
    def compute_hash(self, content: str) -> str:
        """Compute MD5 hash of content."""
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def detect_changes(self, old_item: Dict, new_item: Dict) -> Dict:
        """
        Detect what changed between two versions.
        
        Args:
            old_item: Previous item snapshot
            new_item: New item snapshot
            
        Returns:
            Dict with change flags and diff summary
        """
        changes = {
            'title_changed': old_item.get('title') != new_item.get('title'),
            'content_changed': old_item.get('content_hash') != self.compute_hash(new_item.get('content', '')),
            'meta_changed': old_item.get('meta_description') != new_item.get('meta_description'),
            'status_changed': old_item.get('status_code') != new_item.get('status_code'),
            'length_changed': old_item.get('content_length') != new_item.get('content_length'),
        }
        
        # Compute content diff if changed
        diff_lines = []
        if changes['content_changed']:
            old_content = old_item.get('content', '').splitlines()
            new_content = new_item.get('content', '').splitlines()
            diff = unified_diff(old_content, new_content, lineterm='')
            diff_lines = list(diff)[:50]  # Limit diff size
        
        changes['has_changes'] = any(changes.values())
        changes['diff_preview'] = '\n'.join(diff_lines[:20]) if diff_lines else None
        
        return changes
    
    def save_version(self, item_id: int, item_data: Dict, 
                      version: Optional[int] = None) -> int:
        """
        Save a version snapshot.
        
        Args:
            item_id: Crawled item ID
            item_data: Item data dict
            version: Version number (auto-incremented if None)
            
        Returns:
            Version number saved
        """
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Get next version number if not provided
        if version is None:
            cur.execute('''
                SELECT COALESCE(MAX(version), 0) + 1
                FROM item_history
                WHERE item_id = %s
            ''', (item_id,))
            version = cur.fetchone()[0]
        
        # Get previous version for change detection
        cur.execute('''
            SELECT title, content, meta_description, content_length, status_code, content_hash
            FROM item_history
            WHERE item_id = %s AND version = %s
        ''', (item_id, version - 1))
        
        prev = cur.fetchone()
        
        # Compute content hash
        content_hash = self.compute_hash(item_data.get('content', ''))
        
        # Detect changes
        title_changed = False
        content_changed = False
        meta_changed = False
        
        if prev:
            title_changed = prev[0] != item_data.get('title')
            content_changed = prev[5] != content_hash
            meta_changed = prev[2] != item_data.get('meta_description')
        else:
            # First version - mark as changed
            title_changed = True
            content_changed = True
            meta_changed = True
        
        # Save version
        cur.execute('''
            INSERT INTO item_history 
            (item_id, version, title, content, meta_description, 
             content_length, status_code, title_changed, content_changed, 
             meta_changed, content_hash, crawled_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (
            item_id,
            version,
            item_data.get('title'),
            item_data.get('content'),
            item_data.get('meta_description'),
            item_data.get('content_length'),
            item_data.get('status_code'),
            title_changed,
            content_changed,
            meta_changed,
            content_hash,
            item_data.get('crawled_at', datetime.utcnow())
        ))
        
        history_id = cur.fetchone()[0]
        conn.commit()
        
        cur.close()
        conn.close()
        
        logger.info(f"Saved version {version} for item {item_id}")
        
        return version
    
    def get_history(self, item_id: int) -> List[Dict]:
        """
        Get version history for an item.
        
        Args:
            item_id: Crawled item ID
            
        Returns:
            List of version records
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute('''
            SELECT 
                id, version, title, content_length, status_code,
                title_changed, content_changed, meta_changed,
                crawled_at, recorded_at
            FROM item_history
            WHERE item_id = %s
            ORDER BY version DESC
        ''', (item_id,))
        
        history = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Convert to dict and handle datetime
        result = []
        for row in history:
            item = dict(row)
            if item.get('crawled_at'):
                item['crawled_at'] = item['crawled_at'].isoformat()
            if item.get('recorded_at'):
                item['recorded_at'] = item['recorded_at'].isoformat()
            # Don't include full content in list view
            item.pop('content', None)
            result.append(item)
        
        return result
    
    def get_version(self, item_id: int, version: int) -> Optional[Dict]:
        """
        Get a specific version of an item.
        
        Args:
            item_id: Crawled item ID
            version: Version number
            
        Returns:
            Full item snapshot
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute('''
            SELECT *
            FROM item_history
            WHERE item_id = %s AND version = %s
        ''', (item_id, version))
        
        row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if row:
            item = dict(row)
            if item.get('crawled_at'):
                item['crawled_at'] = item['crawled_at'].isoformat()
            if item.get('recorded_at'):
                item['recorded_at'] = item['recorded_at'].isoformat()
            return item
        
        return None
    
    def compare_versions(self, item_id: int, v1: int, v2: int) -> Dict:
        """
        Compare two versions of an item.
        
        Args:
            item_id: Crawled item ID
            v1: First version number
            v2: Second version number
            
        Returns:
            Comparison result with diff
        """
        version1 = self.get_version(item_id, v1)
        version2 = self.get_version(item_id, v2)
        
        if not version1 or not version2:
            return {'error': 'Version not found'}
        
        # Detect changes
        changes = self.detect_changes(version1, version2)
        
        # Generate side-by-side comparison
        return {
            'item_id': item_id,
            'version1': {
                'number': v1,
                'crawled_at': version1.get('crawled_at'),
                'title': version1.get('title'),
                'content_length': version1.get('content_length'),
                'status_code': version1.get('status_code')
            },
            'version2': {
                'number': v2,
                'crawled_at': version2.get('crawled_at'),
                'title': version2.get('title'),
                'content_length': version2.get('content_length'),
                'status_code': version2.get('status_code')
            },
            'changes': changes,
            'content_diff': changes.get('diff_preview')
        }
    
    def get_changed_items(self, limit: int = 50) -> List[Dict]:
        """
        Get items that have changed between versions.
        
        Args:
            limit: Maximum items to return
            
        Returns:
            List of items with changes
        """
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute('''
            SELECT DISTINCT ON (ih.item_id)
                ih.item_id,
                ci.url,
                ci.domain,
                ih.version,
                ih.title_changed,
                ih.content_changed,
                ih.meta_changed,
                ih.crawled_at
            FROM item_history ih
            JOIN crawled_items ci ON ih.item_id = ci.id
            WHERE ih.title_changed OR ih.content_changed OR ih.meta_changed
            ORDER BY ih.item_id, ih.version DESC
            LIMIT %s
        ''', (limit,))
        
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        result = []
        for row in rows:
            item = dict(row)
            if item.get('crawled_at'):
                item['crawled_at'] = item['crawled_at'].isoformat()
            result.append(item)
        
        return result
    
    def get_stats(self) -> Dict:
        """Get versioning statistics."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Total versions
        cur.execute('SELECT COUNT(*) FROM item_history')
        total_versions = cur.fetchone()[0]
        
        # Items with history
        cur.execute('SELECT COUNT(DISTINCT item_id) FROM item_history')
        items_with_history = cur.fetchone()[0]
        
        # Items with changes
        cur.execute('''
            SELECT COUNT(DISTINCT item_id) 
            FROM item_history 
            WHERE title_changed OR content_changed OR meta_changed
        ''')
        items_changed = cur.fetchone()[0]
        
        # Average versions per item
        cur.execute('''
            SELECT AVG(version_count) FROM (
                SELECT COUNT(*) as version_count 
                FROM item_history 
                GROUP BY item_id
            ) t
        ''')
        avg_versions = cur.fetchone()[0] or 0
        
        cur.close()
        conn.close()
        
        return {
            'total_versions': total_versions,
            'items_with_history': items_with_history,
            'items_with_changes': items_changed,
            'avg_versions_per_item': round(avg_versions, 2),
            'has_data': total_versions > 0
        }
    
    def backfill_from_items(self) -> int:
        """Create initial versions from existing items."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Get items that don't have history
        cur.execute('''
            SELECT ci.id, ci.title, ci.content, ci.meta_description, 
                   ci.content_length, ci.status_code, ci.crawled_at
            FROM crawled_items ci
            LEFT JOIN item_history ih ON ci.id = ih.item_id
            WHERE ih.id IS NULL
        ''')
        
        items = cur.fetchall()
        created = 0
        
        for item in items:
            item_data = {
                'title': item[1],
                'content': item[2],
                'meta_description': item[3],
                'content_length': item[4],
                'status_code': item[5],
                'crawled_at': item[6]
            }
            
            self.save_version(item[0], item_data, version=1)
            created += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Backfilled {created} items with initial versions")
        return created
