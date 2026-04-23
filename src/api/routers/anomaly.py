"""
Anomaly detection API endpoints.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

router = APIRouter(prefix="/api/v1/anomaly", tags=["anomaly"])


class AnomalyResponse(BaseModel):
    item_id: int
    url: str
    is_anomaly: bool
    severity: str
    anomaly_score: float
    details: Dict[str, Any]


@router.post("/baseline/build")
async def build_baseline(background_tasks: BackgroundTasks, limit: int = 100):
    """Build anomaly detection baseline from historical data."""
    from src.analytics.anomaly_detector import get_detector
    
    def build():
        detector = get_detector()
        result = detector.build_baseline_from_database(limit)
        print(f"Baseline built: {result}")
    
    background_tasks.add_task(build)
    
    return {
        "message": f"Building baseline from {limit} items in background"
    }


@router.get("/stats")
async def get_detector_stats():
    """Get anomaly detector statistics."""
    from src.analytics.anomaly_detector import get_detector
    
    detector = get_detector()
    return detector.get_stats()


@router.post("/analyze/{item_id}")
async def analyze_item(item_id: int):
    """Analyze a single item for anomalies."""
    import psycopg2
    from src.analytics.anomaly_detector import get_detector
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    
    # Get item details
    cur.execute('''
        SELECT id, url, title, content_length
        FROM crawled_items
        WHERE id = %s
    ''', (item_id,))
    
    item_row = cur.fetchone()
    if not item_row:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
    
    # Get sentiment
    cur.execute('''
        SELECT sentiment_polarity
        FROM analysis_results
        WHERE item_id = %s
    ''', (item_id,))
    sentiment_row = cur.fetchone()
    
    # Get entities
    cur.execute('''
        SELECT entity_text
        FROM extracted_entities
        WHERE analysis_id IN (
            SELECT id FROM analysis_results WHERE item_id = %s
        )
    ''', (item_id,))
    entities = [r[0] for r in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    # Build item dict
    item = {
        'content_length': item_row[3],
        'sentiment_score': sentiment_row[0] if sentiment_row else 0,
        'entities': entities,
        'topics': {}
    }
    
    # Analyze
    detector = get_detector()
    result = detector.analyze_item(item)
    
    return {
        'item_id': item_id,
        'url': item_row[1],
        'title': item_row[2],
        **result
    }


@router.get("/recent")
async def get_recent_anomalies(limit: int = 20, min_severity: str = "low"):
    """
    Get recent anomalies.
    
    Severity levels: low, medium, high
    """
    import psycopg2
    from src.analytics.anomaly_detector import get_detector
    
    severity_levels = {'low': 0, 'medium': 1, 'high': 2}
    min_level = severity_levels.get(min_severity, 0)
    
    conn = psycopg2.connect(
        host='localhost', port=5433,
        database='tri_layer_crawler',
        user='crawler_user', password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    
    cur.execute('''
        SELECT id, url, title, content_length, crawled_at
        FROM crawled_items
        ORDER BY crawled_at DESC
        LIMIT %s
    ''', (limit * 2,))  # Get more to filter
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    detector = get_detector()
    anomalies = []
    
    for row in rows:
        item = {
            'content_length': row[3],
            'sentiment_score': 0,
            'entities': [],
            'topics': {}
        }
        
        result = detector.analyze_item(item)
        
        if result['is_anomaly']:
            sev_level = severity_levels.get(result['severity'], 0)
            if sev_level >= min_level:
                anomalies.append({
                    'item_id': row[0],
                    'url': row[1],
                    'title': row[2],
                    'crawled_at': row[4].isoformat() if row[4] else None,
                    **result
                })
        
        if len(anomalies) >= limit:
            break
    
    return {
        'count': len(anomalies),
        'min_severity': min_severity,
        'anomalies': anomalies
    }


@router.post("/scan/all")
async def scan_all_items(background_tasks: BackgroundTasks, limit: int = 50):
    """Scan all recent items for anomalies."""
    from src.analytics.anomaly_detector import get_detector
    
    def scan():
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost', port=5433,
            database='tri_layer_crawler',
            user='crawler_user', password='CrawlerPass2024!'
        )
        cur = conn.cursor()
        
        cur.execute('''
            SELECT id, content_length
            FROM crawled_items
            WHERE content_length IS NOT NULL
            ORDER BY crawled_at DESC
            LIMIT %s
        ''', (limit,))
        
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        detector = get_detector()
        
        # First pass: build baseline
        for row in rows[:min(50, len(rows)//2)]:
            if row[1]:
                detector.content_lengths.append(row[1])
        
        # Second pass: detect anomalies
        anomalies_found = 0
        for row in rows:
            item = {'content_length': row[1], 'sentiment_score': 0, 'entities': [], 'topics': {}}
            result = detector.analyze_item(item)
            if result['is_anomaly']:
                anomalies_found += 1
        
        print(f"Scan complete: {anomalies_found} anomalies found in {len(rows)} items")
    
    background_tasks.add_task(scan)
    
    return {"message": f"Scanning {limit} items in background"}
