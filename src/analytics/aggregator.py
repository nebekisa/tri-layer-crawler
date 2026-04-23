"""
Analytics aggregator - combines data from multiple tables.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class AnalyticsAggregator:
    """Aggregate analytics data from multiple tables."""
    
    def __init__(self):
        pass
    
    def get_item_analytics(self, item_id: int) -> Optional[Dict]:
        """Get complete analytics for a single item."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get item with analytics
        cur.execute('''
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
                ar.sentiment_label,
                ar.sentiment_polarity,
                ar.sentiment_subjectivity,
                ar.sentiment_confidence,
                ar.flesch_reading_ease,
                ar.flesch_kincaid_grade,
                ar.reading_time_minutes,
                ar.word_count,
                ar.sentence_count,
                cs.summary
            FROM crawled_items ci
            LEFT JOIN analysis_results ar ON ci.id = ar.item_id
            LEFT JOIN content_summaries cs ON ci.id = cs.item_id
            WHERE ci.id = %s
        ''', (item_id,))
        
        item = cur.fetchone()
        
        if not item:
            cur.close()
            conn.close()
            return None
        
        # Get entities
        cur.execute('''
            SELECT entity_text, entity_type, confidence, occurrence_count
            FROM extracted_entities ee
            JOIN analysis_results ar ON ee.analysis_id = ar.id
            WHERE ar.item_id = %s
            ORDER BY confidence DESC, occurrence_count DESC
            LIMIT 20
        ''', (item_id,))
        entities = cur.fetchall()
        
        # Get keywords
        cur.execute('''
            SELECT keyword, score
            FROM extracted_keywords ek
            JOIN analysis_results ar ON ek.analysis_id = ar.id
            WHERE ar.item_id = %s
            ORDER BY score DESC
            LIMIT 15
        ''', (item_id,))
        keywords = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Build response
        result = dict(item)
        result['entities'] = [dict(e) for e in entities] if entities else []
        result['keywords'] = [dict(k) for k in keywords] if keywords else []
        
        # Format dates
        if result.get('crawled_at'):
            result['crawled_at'] = result['crawled_at'].isoformat()
        
        # Add readability assessment
        if result.get('flesch_reading_ease'):
            ease = result['flesch_reading_ease']
            if ease >= 60:
                result['readability_level'] = 'Easy'
            elif ease >= 30:
                result['readability_level'] = 'Moderate'
            else:
                result['readability_level'] = 'Difficult'
        
        return result
    
    def get_items_analytics(
        self,
        limit: int = 20,
        offset: int = 0,
        domain: Optional[str] = None,
        sentiment: Optional[str] = None,
        keyword: Optional[str] = None,
        has_entities: Optional[bool] = None,
        order_by: str = 'crawled_at',
        order_dir: str = 'DESC'
    ) -> Dict[str, Any]:
        """Get analytics for multiple items with filtering."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build WHERE clauses
        where_clauses = ["1=1"]
        params = []
        
        if domain:
            where_clauses.append("ci.domain = %s")
            params.append(domain)
        
        if sentiment:
            where_clauses.append("ar.sentiment_label = %s")
            params.append(sentiment)
        
        if has_entities is True:
            where_clauses.append("EXISTS (SELECT 1 FROM extracted_entities ee JOIN analysis_results ar2 ON ee.analysis_id = ar2.id WHERE ar2.item_id = ci.id)")
        
        if keyword:
            where_clauses.append("EXISTS (SELECT 1 FROM extracted_keywords ek JOIN analysis_results ar2 ON ek.analysis_id = ar2.id WHERE ar2.item_id = ci.id AND ek.keyword ILIKE %s)")
            params.append(f'%{keyword}%')
        
        where_sql = " AND ".join(where_clauses)
        
        # Validate order_by
        allowed_orders = ['crawled_at', 'id', 'domain', 'sentiment_polarity']
        if order_by not in allowed_orders:
            order_by = 'crawled_at'
        
        if order_dir.upper() not in ('ASC', 'DESC'):
            order_dir = 'DESC'
        
        # Get total count
        count_query = f'''
            SELECT COUNT(*) as total
            FROM crawled_items ci
            LEFT JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE {where_sql}
        '''
        cur.execute(count_query, params)
        total = cur.fetchone()['total']
        
        # Get items
        query = f'''
            SELECT 
                ci.id,
                ci.url,
                ci.title,
                ci.domain,
                ci.content_length,
                ci.crawled_at,
                ar.sentiment_label,
                ar.sentiment_polarity,
                ar.word_count,
                ar.flesch_reading_ease,
                cs.summary
            FROM crawled_items ci
            LEFT JOIN analysis_results ar ON ci.id = ar.item_id
            LEFT JOIN content_summaries cs ON ci.id = cs.item_id
            WHERE {where_sql}
            ORDER BY {order_by} {order_dir}
            LIMIT %s OFFSET %s
        '''
        
        params.extend([limit, offset])
        cur.execute(query, params)
        items = cur.fetchall()
        
        # Get entities and keywords for each item (batch)
        item_ids = [item['id'] for item in items]
        
        entities_map = {}
        keywords_map = {}
        
        if item_ids:
            # Get entities
            cur.execute('''
                SELECT ar.item_id, ee.entity_text, ee.entity_type, ee.confidence
                FROM extracted_entities ee
                JOIN analysis_results ar ON ee.analysis_id = ar.id
                WHERE ar.item_id = ANY(%s)
                ORDER BY ee.confidence DESC
            ''', (item_ids,))
            
            for row in cur.fetchall():
                item_id = row['item_id']
                if item_id not in entities_map:
                    entities_map[item_id] = []
                entities_map[item_id].append({
                    'text': row['entity_text'],
                    'type': row['entity_type'],
                    'confidence': row['confidence']
                })
            
            # Get keywords
            cur.execute('''
                SELECT ar.item_id, ek.keyword, ek.score
                FROM extracted_keywords ek
                JOIN analysis_results ar ON ek.analysis_id = ar.id
                WHERE ar.item_id = ANY(%s)
                ORDER BY ek.score DESC
            ''', (item_ids,))
            
            for row in cur.fetchall():
                item_id = row['item_id']
                if item_id not in keywords_map:
                    keywords_map[item_id] = []
                keywords_map[item_id].append({
                    'keyword': row['keyword'],
                    'score': round(row['score'], 3)
                })
        
        cur.close()
        conn.close()
        
        # Build results
        results = []
        for item in items:
            item_dict = dict(item)
            item_id = item_dict['id']
            
            if item_dict.get('crawled_at'):
                item_dict['crawled_at'] = item_dict['crawled_at'].isoformat()
            
            item_dict['entities'] = entities_map.get(item_id, [])[:10]
            item_dict['keywords'] = keywords_map.get(item_id, [])[:8]
            
            results.append(item_dict)
        
        return {
            'total': total,
            'limit': limit,
            'offset': offset,
            'count': len(results),
            'items': results
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get aggregate analytics summary."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        summary = {}
        
        # Overall stats
        cur.execute('''
            SELECT 
                COUNT(DISTINCT ci.id) as total_items,
                COUNT(DISTINCT ci.domain) as total_domains,
                AVG(ar.sentiment_polarity) as avg_sentiment,
                AVG(ar.word_count) as avg_word_count,
                AVG(ar.flesch_reading_ease) as avg_readability,
                SUM(CASE WHEN ar.sentiment_label = 'positive' THEN 1 ELSE 0 END) as positive_count,
                SUM(CASE WHEN ar.sentiment_label = 'neutral' THEN 1 ELSE 0 END) as neutral_count,
                SUM(CASE WHEN ar.sentiment_label = 'negative' THEN 1 ELSE 0 END) as negative_count
            FROM crawled_items ci
            LEFT JOIN analysis_results ar ON ci.id = ar.item_id
        ''')
        overall = cur.fetchone()
        summary['overall'] = dict(overall)
        
        # Top domains
        cur.execute('''
            SELECT domain, COUNT(*) as count
            FROM crawled_items
            GROUP BY domain
            ORDER BY count DESC
            LIMIT 10
        ''')
        summary['top_domains'] = cur.fetchall()
        
        # Top entities
        cur.execute('''
            SELECT entity_text, entity_type, COUNT(*) as count
            FROM extracted_entities
            GROUP BY entity_text, entity_type
            ORDER BY count DESC
            LIMIT 15
        ''')
        summary['top_entities'] = cur.fetchall()
        
        # Top keywords
        cur.execute('''
            SELECT keyword, AVG(score) as avg_score, COUNT(*) as count
            FROM extracted_keywords
            GROUP BY keyword
            ORDER BY count DESC, avg_score DESC
            LIMIT 15
        ''')
        summary['top_keywords'] = cur.fetchall()
        
        # Recent activity
        cur.execute('''
            SELECT DATE(crawled_at) as date, COUNT(*) as count
            FROM crawled_items
            WHERE crawled_at IS NOT NULL
            GROUP BY DATE(crawled_at)
            ORDER BY date DESC
            LIMIT 7
        ''')
        summary['recent_activity'] = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return summary
