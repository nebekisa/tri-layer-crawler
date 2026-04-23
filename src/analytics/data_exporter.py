"""
Multi-format data export for crawled content.
Fixed with correct schema columns.
"""

import csv
import json
import logging
from io import StringIO, BytesIO
from typing import List, Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DataExporter:
    """Export crawled data in multiple formats."""
    
    def __init__(self):
        pass
    
    def get_all_items(self, limit: Optional[int] = None, 
                       domain: Optional[str] = None,
                       from_date: Optional[str] = None,
                       to_date: Optional[str] = None) -> List[Dict]:
        """Fetch items from database with filters."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build query with existing columns only
        query = '''
            SELECT 
                ci.id,
                ci.url,
                ci.title,
                ci.content,
                ci.meta_description,
                ci.domain,
                ci.status_code,
                ci.content_length,
                ci.crawled_at,
                ar.sentiment_polarity,
                ar.sentiment_subjectivity,
                cs.summary
            FROM crawled_items ci
            LEFT JOIN analysis_results ar ON ci.id = ar.item_id
            LEFT JOIN content_summaries cs ON ci.id = cs.item_id
            WHERE 1=1
        '''
        params = []
        
        if domain:
            query += ' AND ci.domain = %s'
            params.append(domain)
        
        if from_date:
            query += ' AND ci.crawled_at >= %s'
            params.append(from_date)
        
        if to_date:
            query += ' AND ci.crawled_at <= %s'
            params.append(to_date)
        
        query += ' ORDER BY ci.crawled_at DESC'
        
        if limit:
            query += ' LIMIT %s'
            params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Convert to dict and handle datetime
        items = []
        for row in rows:
            item = dict(row)
            if item.get('crawled_at'):
                item['crawled_at'] = item['crawled_at'].isoformat()
            items.append(item)
        
        return items
    
    def get_entities(self, item_ids: List[int]) -> Dict[int, List[Dict]]:
        """Get entities grouped by item_id."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        if not item_ids:
            return {}
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get analysis_ids first
        cur.execute('''
            SELECT id, item_id FROM analysis_results WHERE item_id = ANY(%s)
        ''', (item_ids,))
        
        analysis_map = {}
        for row in cur.fetchall():
            analysis_map[row['id']] = row['item_id']
        
        if not analysis_map:
            return {}
        
        # Get entities
        cur.execute('''
            SELECT analysis_id, entity_text, entity_type, confidence, occurrence_count
            FROM extracted_entities
            WHERE analysis_id = ANY(%s)
        ''', (list(analysis_map.keys()),))
        
        entities_by_item = {item_id: [] for item_id in item_ids}
        for row in cur.fetchall():
            item_id = analysis_map.get(row['analysis_id'])
            if item_id:
                entities_by_item[item_id].append(dict(row))
        
        cur.close()
        conn.close()
        
        return entities_by_item
    
    def export_csv(self, limit: Optional[int] = 1000, **filters) -> str:
        """Export to CSV format."""
        items = self.get_all_items(limit=limit, **filters)
        
        if not items:
            return "id,url,title,domain,crawled_at\n"
        
        output = StringIO()
        
        # Flatten entities into strings
        entity_map = self.get_entities([i['id'] for i in items])
        
        for item in items:
            entities = entity_map.get(item['id'], [])
            item['entities'] = '; '.join([
                f"{e['entity_text']}({e['entity_type']})" 
                for e in entities[:10]
            ])
        
        # Define columns that actually exist
        columns = [
            'id', 'url', 'title', 'domain', 'status_code', 
            'content_length', 'crawled_at', 'sentiment_polarity',
            'sentiment_subjectivity', 'summary', 'entities', 'meta_description'
        ]
        
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(items)
        
        return output.getvalue()
    
    def export_json(self, limit: Optional[int] = 1000, 
                     include_entities: bool = True, **filters) -> str:
        """Export to JSON format."""
        items = self.get_all_items(limit=limit, **filters)
        
        if include_entities and items:
            entity_map = self.get_entities([i['id'] for i in items])
            for item in items:
                item['entities'] = entity_map.get(item['id'], [])
        
        export_data = {
            'exported_at': datetime.utcnow().isoformat(),
            'total': len(items),
            'filters': filters,
            'items': items
        }
        
        return json.dumps(export_data, indent=2, ensure_ascii=False)
    
    def export_ndjson(self, limit: Optional[int] = 1000, **filters) -> str:
        """Export to NDJSON format."""
        items = self.get_all_items(limit=limit, **filters)
        
        lines = []
        for item in items:
            lines.append(json.dumps(item, ensure_ascii=False))
        
        return '\n'.join(lines)
    
    def export_parquet(self, limit: Optional[int] = 1000, **filters) -> bytes:
        """Export to Parquet format."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow required for Parquet export. Install: pip install pyarrow")
        
        items = self.get_all_items(limit=limit, **filters)
        
        if not items:
            return b""
        
        # Build column arrays
        arrays = {}
        if items:
            for col in items[0].keys():
                values = [item.get(col) for item in items]
                try:
                    arrays[col] = pa.array(values)
                except:
                    arrays[col] = pa.array([str(v) if v is not None else None for v in values])
        
        table = pa.Table.from_pydict(arrays)
        output = BytesIO()
        pq.write_table(table, output)
        
        return output.getvalue()
    
    def get_stats(self) -> Dict:
        """Get export statistics."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        cur.execute('SELECT COUNT(*) FROM crawled_items')
        total_items = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(DISTINCT domain) FROM crawled_items')
        total_domains = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(*) FROM extracted_entities')
        total_entities = cur.fetchone()[0]
        
        cur.execute('SELECT MIN(crawled_at), MAX(crawled_at) FROM crawled_items')
        min_date, max_date = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return {
            'total_items': total_items,
            'total_domains': total_domains,
            'total_entities': total_entities,
            'date_range': {
                'from': min_date.isoformat() if min_date else None,
                'to': max_date.isoformat() if max_date else None
            },
            'export_formats': ['csv', 'json', 'ndjson', 'parquet']
        }
