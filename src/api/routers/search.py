"""
Search API endpoints for Elasticsearch integration.
"""

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/api/v1/search", tags=["search"])


class SearchResult(BaseModel):
    url: str
    title: str
    content_preview: str
    domain: str
    score: float


class SearchResponse(BaseModel):
    total: int
    hits: List[SearchResult]
    took_ms: int
    page: int
    page_size: int


@router.get("/", response_model=SearchResponse)
async def search(
    q: str = Query(..., description="Search query", min_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100)
):
    """Search crawled content."""
    try:
        from src.search.elastic_client import ElasticClient
        client = ElasticClient()
        
        from_ = (page - 1) * page_size
        results = client.search(q, size=page_size, from_=from_)
        
        hits = []
        for hit in results.get('hits', []):
            hits.append(SearchResult(
                url=hit['url'],
                title=hit['title'],
                content_preview=hit['content_preview'],
                domain=hit['domain'],
                score=hit['score']
            ))
        
        return SearchResponse(
            total=results.get('total', 0),
            hits=hits,
            took_ms=results.get('took_ms', 0),
            page=page,
            page_size=page_size
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def search_stats():
    """Get search index statistics."""
    try:
        from src.search.elastic_client import ElasticClient
        client = ElasticClient()
        return client.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
