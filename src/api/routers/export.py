"""
Export API endpoints for crawled data.
"""

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse, Response
from typing import Optional
from io import BytesIO
from datetime import datetime

router = APIRouter(prefix="/api/v1/export", tags=["export"])


@router.get("/stats")
async def get_export_stats():
    """Get export statistics and available data."""
    from src.analytics.data_exporter import DataExporter
    
    exporter = DataExporter()
    return exporter.get_stats()


@router.get("/csv")
async def export_csv(
    limit: Optional[int] = Query(1000, le=10000, description="Maximum items to export"),
    domain: Optional[str] = Query(None, description="Filter by domain"),
    from_date: Optional[str] = Query(None, description="From date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="To date (YYYY-MM-DD)")
):
    """
    Export crawled data as CSV.
    
    - Compatible with Excel, Google Sheets
    - Includes entities as semicolon-separated string
    """
    from src.analytics.data_exporter import DataExporter
    
    exporter = DataExporter()
    
    filters = {}
    if domain:
        filters['domain'] = domain
    if from_date:
        filters['from_date'] = from_date
    if to_date:
        filters['to_date'] = to_date
    
    csv_data = exporter.export_csv(limit=limit, **filters)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"tri_layer_export_{timestamp}.csv"
    
    return Response(
        content=csv_data.encode('utf-8-sig'),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/json")
async def export_json(
    limit: Optional[int] = Query(1000, le=10000, description="Maximum items to export"),
    domain: Optional[str] = Query(None, description="Filter by domain"),
    include_entities: bool = Query(True, description="Include extracted entities"),
    from_date: Optional[str] = Query(None, description="From date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="To date (YYYY-MM-DD)")
):
    """
    Export crawled data as JSON.
    
    - Full structured data
    - Includes entities if requested
    """
    from src.analytics.data_exporter import DataExporter
    
    exporter = DataExporter()
    
    filters = {}
    if domain:
        filters['domain'] = domain
    if from_date:
        filters['from_date'] = from_date
    if to_date:
        filters['to_date'] = to_date
    
    json_data = exporter.export_json(limit=limit, include_entities=include_entities, **filters)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"tri_layer_export_{timestamp}.json"
    
    return Response(
        content=json_data.encode('utf-8'),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/ndjson")
async def export_ndjson(
    limit: Optional[int] = Query(10000, le=50000, description="Maximum items to export"),
    domain: Optional[str] = Query(None, description="Filter by domain")
):
    """
    Export crawled data as NDJSON (Newline Delimited JSON).
    
    - One JSON object per line
    - Ideal for streaming and log processing
    """
    from src.analytics.data_exporter import DataExporter
    
    exporter = DataExporter()
    ndjson_data = exporter.export_ndjson(limit=limit, domain=domain if domain else None)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"tri_layer_export_{timestamp}.ndjson"
    
    return Response(
        content=ndjson_data.encode('utf-8'),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/parquet")
async def export_parquet(
    limit: Optional[int] = Query(5000, le=50000, description="Maximum items to export"),
    domain: Optional[str] = Query(None, description="Filter by domain")
):
    """
    Export crawled data as Parquet.
    
    - Columnar format for data science
    - Compatible with pandas, Spark, DuckDB
    - Requires pyarrow library
    """
    from src.analytics.data_exporter import DataExporter
    
    try:
        exporter = DataExporter()
        parquet_data = exporter.export_parquet(limit=limit, domain=domain if domain else None)
    except ImportError as e:
        raise HTTPException(
            status_code=500, 
            detail="Parquet export requires pyarrow. Install: pip install pyarrow"
        )
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"tri_layer_export_{timestamp}.parquet"
    
    return Response(
        content=parquet_data,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.get("/domains")
async def get_domains():
    """Get list of crawled domains for filtering."""
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
    
    return {
        'total': len(domains),
        'domains': domains
    }
