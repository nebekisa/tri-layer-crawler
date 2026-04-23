"""
Time-series sentiment trend analysis.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

logger = logging.getLogger(__name__)


class SentimentTrendAnalyzer:
    """Analyze sentiment trends over time."""
    
    def __init__(self):
        pass
    
    def get_daily_sentiment(self, days: int = 30, domain: Optional[str] = None) -> List[Dict]:
        """Get daily aggregated sentiment scores."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = '''
            SELECT 
                DATE(ci.crawled_at) as date,
                COUNT(*) as item_count,
                AVG(ar.sentiment_polarity) as avg_polarity,
                AVG(ar.sentiment_subjectivity) as avg_subjectivity,
                COUNT(CASE WHEN ar.sentiment_label = 'positive' THEN 1 END) as positive_count,
                COUNT(CASE WHEN ar.sentiment_label = 'negative' THEN 1 END) as negative_count,
                COUNT(CASE WHEN ar.sentiment_label = 'neutral' THEN 1 END) as neutral_count
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ci.crawled_at >= NOW() - INTERVAL '%s days'
        '''
        params = [days]
        
        if domain:
            query += ' AND ci.domain = %s'
            params.append(domain)
        
        query += ' GROUP BY DATE(ci.crawled_at) ORDER BY date DESC'
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Calculate 7-day moving average
        result = []
        polarities = [r['avg_polarity'] for r in rows]
        
        for i, row in enumerate(rows):
            item = dict(row)
            item['date'] = item['date'].isoformat()
            item['avg_polarity'] = round(item['avg_polarity'], 4) if item['avg_polarity'] else 0
            item['avg_subjectivity'] = round(item['avg_subjectivity'], 4) if item['avg_subjectivity'] else 0
            
            # 7-day moving average
            if i < len(polarities) - 6:
                window = polarities[i:i+7]
                item['moving_avg_7d'] = round(statistics.mean([p for p in window if p is not None]), 4)
            else:
                item['moving_avg_7d'] = None
            
            # Trend direction
            if i < len(polarities) - 1:
                if polarities[i] > polarities[i+1]:
                    item['trend'] = 'up'
                elif polarities[i] < polarities[i+1]:
                    item['trend'] = 'down'
                else:
                    item['trend'] = 'stable'
            else:
                item['trend'] = 'stable'
            
            result.append(item)
        
        return result
    
    def get_domain_comparison(self, days: int = 30, min_items: int = 1) -> List[Dict]:
        """Compare sentiment across domains."""
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
                ci.domain,
                COUNT(*) as item_count,
                AVG(ar.sentiment_polarity) as avg_polarity,
                AVG(ar.sentiment_subjectivity) as avg_subjectivity,
                MAX(ar.sentiment_polarity) as max_polarity,
                MIN(ar.sentiment_polarity) as min_polarity,
                COUNT(CASE WHEN ar.sentiment_label = 'positive' THEN 1 END) as positive,
                COUNT(CASE WHEN ar.sentiment_label = 'negative' THEN 1 END) as negative,
                COUNT(CASE WHEN ar.sentiment_label = 'neutral' THEN 1 END) as neutral,
                MAX(ci.crawled_at) as last_crawled
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ci.crawled_at >= NOW() - INTERVAL '%s days'
            GROUP BY ci.domain
            HAVING COUNT(*) >= %s
            ORDER BY avg_polarity DESC
        ''', (days, min_items))
        
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        result = []
        for row in rows:
            item = dict(row)
            item['avg_polarity'] = round(item['avg_polarity'], 4) if item['avg_polarity'] else 0
            item['avg_subjectivity'] = round(item['avg_subjectivity'], 4) if item['avg_subjectivity'] else 0
            item['last_crawled'] = item['last_crawled'].isoformat() if item['last_crawled'] else None
            
            # Determine sentiment category
            if item['avg_polarity'] > 0.1:
                item['sentiment_category'] = 'positive'
            elif item['avg_polarity'] < -0.1:
                item['sentiment_category'] = 'negative'
            else:
                item['sentiment_category'] = 'neutral'
            
            result.append(item)
        
        return result
    
    def get_sentiment_summary(self, days: int = 30) -> Dict:
        """Get overall sentiment summary."""
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        # Overall stats
        cur.execute('''
            SELECT 
                COUNT(*) as total_items,
                AVG(ar.sentiment_polarity) as avg_polarity,
                STDDEV(ar.sentiment_polarity) as stddev_polarity,
                COUNT(CASE WHEN ar.sentiment_label = 'positive' THEN 1 END) as positive,
                COUNT(CASE WHEN ar.sentiment_label = 'negative' THEN 1 END) as negative,
                COUNT(CASE WHEN ar.sentiment_label = 'neutral' THEN 1 END) as neutral
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ci.crawled_at >= NOW() - INTERVAL '%s days'
        ''', (days,))
        
        row = cur.fetchone()
        
        # Weekly trend
        cur.execute('''
            SELECT 
                DATE_TRUNC('week', ci.crawled_at) as week,
                AVG(ar.sentiment_polarity) as avg_polarity
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ci.crawled_at >= NOW() - INTERVAL '%s days'
            GROUP BY week
            ORDER BY week DESC
            LIMIT 4
        ''', (days,))
        
        weeks = cur.fetchall()
        
        cur.close()
        conn.close()
        
        total = row[0] or 0
        
        # Calculate trend
        week_values = [w[1] for w in weeks if w[1] is not None]
        trend = 'stable'
        if len(week_values) >= 2:
            if week_values[0] > week_values[-1]:
                trend = 'improving'
            elif week_values[0] < week_values[-1]:
                trend = 'declining'
        
        return {
            'period_days': days,
            'total_items': total,
            'avg_polarity': round(row[1], 4) if row[1] else 0,
            'stddev_polarity': round(row[2], 4) if row[2] else 0,
            'distribution': {
                'positive': row[3] or 0,
                'negative': row[4] or 0,
                'neutral': row[5] or 0
            },
            'positive_pct': round((row[3] or 0) / total * 100, 1) if total > 0 else 0,
            'negative_pct': round((row[4] or 0) / total * 100, 1) if total > 0 else 0,
            'neutral_pct': round((row[5] or 0) / total * 100, 1) if total > 0 else 0,
            'weekly_trend': trend,
            'weekly_values': [
                {'week': w[0].isoformat() if w[0] else None, 'avg_polarity': round(w[1], 4) if w[1] else 0}
                for w in weeks
            ]
        }
    
    def get_top_positive_items(self, limit: int = 10, domain: Optional[str] = None) -> List[Dict]:
        """Get items with highest positive sentiment."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = '''
            SELECT 
                ci.id,
                ci.url,
                ci.title,
                ci.domain,
                ar.sentiment_polarity,
                ar.sentiment_subjectivity,
                ar.sentiment_label,
                ci.crawled_at
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ar.sentiment_polarity > 0.3
        '''
        params = []
        
        if domain:
            query += ' AND ci.domain = %s'
            params.append(domain)
        
        query += ' ORDER BY ar.sentiment_polarity DESC LIMIT %s'
        params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        result = []
        for row in rows:
            item = dict(row)
            item['sentiment_polarity'] = round(item['sentiment_polarity'], 4)
            item['crawled_at'] = item['crawled_at'].isoformat() if item['crawled_at'] else None
            result.append(item)
        
        return result
    
    def get_top_negative_items(self, limit: int = 10, domain: Optional[str] = None) -> List[Dict]:
        """Get items with most negative sentiment."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = '''
            SELECT 
                ci.id,
                ci.url,
                ci.title,
                ci.domain,
                ar.sentiment_polarity,
                ar.sentiment_subjectivity,
                ar.sentiment_label,
                ci.crawled_at
            FROM crawled_items ci
            JOIN analysis_results ar ON ci.id = ar.item_id
            WHERE ar.sentiment_polarity < -0.1
        '''
        params = []
        
        if domain:
            query += ' AND ci.domain = %s'
            params.append(domain)
        
        query += ' ORDER BY ar.sentiment_polarity ASC LIMIT %s'
        params.append(limit)
        
        cur.execute(query, params)
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        result = []
        for row in rows:
            item = dict(row)
            item['sentiment_polarity'] = round(item['sentiment_polarity'], 4)
            item['crawled_at'] = item['crawled_at'].isoformat() if item['crawled_at'] else None
            result.append(item)
        
        return result
    
    def detect_sentiment_anomalies(self, days: int = 30, threshold: float = 2.0) -> List[Dict]:
        """Detect days with anomalous sentiment."""
        daily = self.get_daily_sentiment(days=days)
        
        if len(daily) < 7:
            return []
        
        polarities = [d['avg_polarity'] for d in daily if d['avg_polarity'] is not None]
        mean = statistics.mean(polarities)
        std = statistics.stdev(polarities) if len(polarities) > 1 else 0.1
        
        anomalies = []
        for d in daily:
            if d['avg_polarity']:
                z_score = abs(d['avg_polarity'] - mean) / std if std > 0 else 0
                if z_score > threshold:
                    anomalies.append({
                        'date': d['date'],
                        'polarity': d['avg_polarity'],
                        'z_score': round(z_score, 2),
                        'item_count': d['item_count'],
                        'severity': 'high' if z_score > 3 else 'medium'
                    })
        
        return anomalies
