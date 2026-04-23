"""
Sentiment trend analysis API endpoints.
"""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter(prefix="/api/v1/sentiment", tags=["sentiment"])


@router.get("/summary")
async def get_sentiment_summary(days: int = Query(30, ge=1, le=365)):
    """Get overall sentiment summary."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.get_sentiment_summary(days=days)


@router.get("/daily")
async def get_daily_sentiment(
    days: int = Query(30, ge=1, le=90),
    domain: Optional[str] = Query(None)
):
    """Get daily sentiment trends with moving average."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.get_daily_sentiment(days=days, domain=domain)


@router.get("/domains")
async def get_domain_comparison(
    days: int = Query(30, ge=1, le=90),
    min_items: int = Query(1, ge=1)
):
    """Compare sentiment across domains."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.get_domain_comparison(days=days, min_items=min_items)


@router.get("/top/positive")
async def get_top_positive(
    limit: int = Query(10, ge=1, le=50),
    domain: Optional[str] = Query(None)
):
    """Get items with highest positive sentiment."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.get_top_positive_items(limit=limit, domain=domain)


@router.get("/top/negative")
async def get_top_negative(
    limit: int = Query(10, ge=1, le=50),
    domain: Optional[str] = Query(None)
):
    """Get items with most negative sentiment."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.get_top_negative_items(limit=limit, domain=domain)


@router.get("/anomalies")
async def get_sentiment_anomalies(
    days: int = Query(30, ge=7, le=90),
    threshold: float = Query(2.0, ge=1.0, le=4.0)
):
    """Detect days with anomalous sentiment."""
    from src.analytics.sentiment_trends import SentimentTrendAnalyzer
    
    analyzer = SentimentTrendAnalyzer()
    return analyzer.detect_sentiment_anomalies(days=days, threshold=threshold)
