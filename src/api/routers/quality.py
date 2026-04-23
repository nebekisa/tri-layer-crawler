"""
Data Quality Dashboard API - Final Polish
"""

from fastapi import APIRouter, Query
from typing import Optional, List, Dict
from datetime import datetime, timedelta

router = APIRouter(prefix="/api/v1/quality", tags=["quality"])


@router.get("/overview")
async def get_quality_overview():
    """Get comprehensive data quality overview."""
    import psycopg2
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    
    metrics = {}
    
    # Total items
    cur.execute('SELECT COUNT(*) FROM crawled_items')
    metrics['total_items'] = cur.fetchone()[0]
    
    # Items with content
    cur.execute('SELECT COUNT(*) FROM crawled_items WHERE content IS NOT NULL AND LENGTH(content) > 100')
    metrics['items_with_content'] = cur.fetchone()[0]
    
    # Items with entities
    cur.execute('''
        SELECT COUNT(DISTINCT ci.id) 
        FROM crawled_items ci
        JOIN analysis_results ar ON ci.id = ar.item_id
        JOIN extracted_entities ee ON ar.id = ee.analysis_id
    ''')
    metrics['items_with_entities'] = cur.fetchone()[0]
    
    # Items with sentiment
    cur.execute('SELECT COUNT(*) FROM analysis_results WHERE sentiment_polarity IS NOT NULL')
    metrics['items_with_sentiment'] = cur.fetchone()[0]
    
    # Items with summaries
    cur.execute('SELECT COUNT(*) FROM content_summaries')
    metrics['items_with_summaries'] = cur.fetchone()[0]
    
    # Items with topics
    cur.execute('SELECT COUNT(*) FROM item_topics')
    metrics['items_with_topics'] = cur.fetchone()[0]
    
    # Items with history
    cur.execute('SELECT COUNT(DISTINCT item_id) FROM item_history')
    metrics['items_with_history'] = cur.fetchone()[0]
    
    # Recent crawl rate (last 24h)
    cur.execute('''
        SELECT COUNT(*) FROM crawled_items 
        WHERE crawled_at >= NOW() - INTERVAL '24 hours'
    ''')
    metrics['crawled_last_24h'] = cur.fetchone()[0]
    
    # Failed crawls (status != 200)
    cur.execute('SELECT COUNT(*) FROM crawled_items WHERE status_code != 200')
    metrics['failed_crawls'] = cur.fetchone()[0]
    
    # Queue stats
    try:
        from src.queue.redis_queue import RedisQueue
        queue = RedisQueue()
        q_stats = queue.get_stats()
        metrics['queue_pending'] = q_stats['pending']
        metrics['queue_dlq'] = q_stats['dlq']
        queue.close()
    except:
        metrics['queue_pending'] = 0
        metrics['queue_dlq'] = 0
    
    cur.close()
    conn.close()
    
    # Calculate scores
    total = metrics['total_items']
    metrics['content_coverage'] = round(metrics['items_with_content'] / total * 100, 1) if total > 0 else 0
    metrics['entity_coverage'] = round(metrics['items_with_entities'] / total * 100, 1) if total > 0 else 0
    metrics['sentiment_coverage'] = round(metrics['items_with_sentiment'] / total * 100, 1) if total > 0 else 0
    metrics['success_rate'] = round((total - metrics['failed_crawls']) / total * 100, 1) if total > 0 else 0
    
    # Overall quality score (0-100)
    quality_score = (
        metrics['content_coverage'] * 0.25 +
        metrics['entity_coverage'] * 0.20 +
        metrics['sentiment_coverage'] * 0.15 +
        metrics['success_rate'] * 0.40
    )
    metrics['overall_quality_score'] = round(quality_score, 1)
    
    return metrics


@router.get("/issues")
async def get_quality_issues(limit: int = Query(50, le=200)):
    """Get list of data quality issues."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    issues = []
    
    # Items with no content
    cur.execute('''
        SELECT id, url, domain, crawled_at, 'no_content' as issue_type
        FROM crawled_items
        WHERE content IS NULL OR LENGTH(content) < 100
        ORDER BY crawled_at DESC
        LIMIT %s
    ''', (limit // 4,))
    for row in cur.fetchall():
        row['crawled_at'] = row['crawled_at'].isoformat() if row['crawled_at'] else None
        issues.append(dict(row))
    
    # Failed crawls
    cur.execute('''
        SELECT id, url, domain, crawled_at, status_code, 'failed_crawl' as issue_type
        FROM crawled_items
        WHERE status_code != 200
        ORDER BY crawled_at DESC
        LIMIT %s
    ''', (limit // 4,))
    for row in cur.fetchall():
        row['crawled_at'] = row['crawled_at'].isoformat() if row['crawled_at'] else None
        issues.append(dict(row))
    
    # Items without entities
    cur.execute('''
        SELECT ci.id, ci.url, ci.domain, ci.crawled_at, 'no_entities' as issue_type
        FROM crawled_items ci
        LEFT JOIN analysis_results ar ON ci.id = ar.item_id
        LEFT JOIN extracted_entities ee ON ar.id = ee.analysis_id
        WHERE ee.id IS NULL
        ORDER BY ci.crawled_at DESC
        LIMIT %s
    ''', (limit // 4,))
    for row in cur.fetchall():
        row['crawled_at'] = row['crawled_at'].isoformat() if row['crawled_at'] else None
        issues.append(dict(row))
    
    cur.close()
    conn.close()
    
    return {
        'total_issues': len(issues),
        'issues': issues[:limit]
    }


@router.get("/trends")
async def get_quality_trends(days: int = Query(7, ge=1, le=30)):
    """Get quality metrics trends over time."""
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
            DATE(crawled_at) as date,
            COUNT(*) as total,
            COUNT(CASE WHEN status_code = 200 THEN 1 END) as successful,
            AVG(CASE WHEN content_length > 0 THEN content_length END) as avg_content_length
        FROM crawled_items
        WHERE crawled_at >= NOW() - INTERVAL '%s days'
        GROUP BY DATE(crawled_at)
        ORDER BY date DESC
    ''', (days,))
    
    trends = []
    for row in cur.fetchall():
        item = dict(row)
        item['date'] = item['date'].isoformat()
        item['success_rate'] = round(item['successful'] / item['total'] * 100, 1) if item['total'] > 0 else 0
        trends.append(item)
    
    cur.close()
    conn.close()
    
    return trends
