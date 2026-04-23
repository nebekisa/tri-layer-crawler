"""
Analytics API endpoints - Unified view of all data.
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])


class ItemAnalyticsResponse(BaseModel):
    id: int
    url: str
    title: str
    domain: str
    content_length: Optional[int]
    crawled_at: Optional[str]
    sentiment_label: Optional[str]
    sentiment_polarity: Optional[float]
    word_count: Optional[int]
    flesch_reading_ease: Optional[float]
    readability_level: Optional[str]
    summary: Optional[str]
    entities: List[Dict] = []
    keywords: List[Dict] = []


class ItemsListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    count: int
    items: List[Dict]


@router.get("/item/{item_id}", response_model=ItemAnalyticsResponse)
async def get_item_analytics(item_id: int):
    """Get complete analytics for a single item."""
    from src.analytics.aggregator import AnalyticsAggregator
    
    aggregator = AnalyticsAggregator()
    result = aggregator.get_item_analytics(item_id)
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
    
    return result


@router.get("/items", response_model=ItemsListResponse)
async def get_items_analytics(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    domain: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None, regex="^(positive|neutral|negative)$"),
    keyword: Optional[str] = Query(None),
    has_entities: Optional[bool] = Query(None),
    order_by: str = Query("crawled_at", regex="^(crawled_at|id|domain|sentiment_polarity)$"),
    order_dir: str = Query("DESC", regex="^(ASC|DESC)$")
):
    """
    Get analytics for multiple items with filtering and pagination.
    
    - **domain**: Filter by domain (e.g., books.toscrape.com)
    - **sentiment**: Filter by sentiment (positive, neutral, negative)
    - **keyword**: Filter items containing this keyword
    - **has_entities**: Only show items with extracted entities
    - **limit**: Items per page (max 100)
    - **offset**: Pagination offset
    """
    from src.analytics.aggregator import AnalyticsAggregator
    
    aggregator = AnalyticsAggregator()
    result = aggregator.get_items_analytics(
        limit=limit,
        offset=offset,
        domain=domain,
        sentiment=sentiment,
        keyword=keyword,
        has_entities=has_entities,
        order_by=order_by,
        order_dir=order_dir
    )
    
    return result


@router.get("/summary")
async def get_analytics_summary():
    """
    Get aggregate analytics summary.
    
    Returns:
    - Overall statistics (total items, avg sentiment, etc.)
    - Top domains
    - Top entities
    - Top keywords
    - Recent activity
    """
    from src.analytics.aggregator import AnalyticsAggregator
    
    aggregator = AnalyticsAggregator()
    return aggregator.get_summary()


@router.get("/domains")
async def get_domains():
    """Get list of crawled domains with counts."""
    import psycopg2
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    
    cur.execute('''
        SELECT domain, COUNT(*) as count, MAX(crawled_at) as last_crawled
        FROM crawled_items
        GROUP BY domain
        ORDER BY count DESC
    ''')
    
    domains = []
    for row in cur.fetchall():
        domains.append({
            'domain': row[0],
            'count': row[1],
            'last_crawled': row[2].isoformat() if row[2] else None
        })
    
    cur.close()
    conn.close()
    
    return {'total': len(domains), 'domains': domains}


@router.get("/sentiment/distribution")
async def get_sentiment_distribution():
    """Get sentiment distribution across all items."""
    import psycopg2
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    
    cur.execute('''
        SELECT 
            sentiment_label,
            COUNT(*) as count,
            ROUND(AVG(sentiment_polarity)::numeric, 3) as avg_polarity,
            ROUND(AVG(sentiment_confidence)::numeric, 3) as avg_confidence
        FROM analysis_results
        WHERE sentiment_label IS NOT NULL
        GROUP BY sentiment_label
        ORDER BY count DESC
    ''')
    
    distribution = []
    for row in cur.fetchall():
        distribution.append({
            'label': row[0],
            'count': row[1],
            'avg_polarity': float(row[2]) if row[2] else 0,
            'avg_confidence': float(row[3]) if row[3] else 0
        })
    
    cur.close()
    conn.close()
    
    return {'distribution': distribution}
