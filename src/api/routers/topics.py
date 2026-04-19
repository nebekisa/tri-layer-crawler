"""
Topic modeling API endpoints.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/api/v1/topics", tags=["topics"])


class TopicInfo(BaseModel):
    topic_id: int
    label: str
    keywords: List[str]
    count: int


class TopicDistribution(BaseModel):
    topics: List[dict]


class SimilarDocumentResult(BaseModel):
    item_id: int
    url: str
    title: str
    similarity: float


@router.post("/fit")
async def fit_topic_model(background_tasks: BackgroundTasks):
    """
    Fit topic model on all crawled content.
    
    Runs in background due to processing time.
    """
    from src.analytics.topic_modeler import TopicService
    
    def fit_model():
        service = TopicService()
        result = service.fit_on_all_items()
        print(f"Topic modeling complete: {result}")
    
    background_tasks.add_task(fit_model)
    
    return {"message": "Topic modeling started in background"}


@router.get("/info", response_model=List[TopicInfo])
async def get_topic_info():
    """Get information about discovered topics."""
    from src.analytics.topic_modeler import TopicModeler
    
    topics = TopicModeler.get_topic_info()
    
    if not topics:
        raise HTTPException(status_code=404, detail="No topics found. Run /fit first.")
    
    return topics


@router.get("/distribution")
async def get_topic_distribution():
    """Get distribution of topics across items."""
    from src.analytics.topic_modeler import TopicService
    
    service = TopicService()
    return service.get_topic_distribution()


@router.get("/items/{topic_id}")
async def get_items_by_topic(topic_id: int, limit: int = 10):
    """Get items assigned to a specific topic."""
    from src.analytics.topic_modeler import TopicService
    
    service = TopicService()
    items = service.get_items_by_topic(topic_id, limit)
    
    if not items:
        raise HTTPException(status_code=404, detail=f"No items found for topic {topic_id}")
    
    return items


@router.post("/similar")
async def find_similar_documents(query: str, top_k: int = 5):
    """
    Find documents similar to query using topic embeddings.
    """
    import psycopg2
    from src.analytics.topic_modeler import TopicModeler
    
    # Get all documents
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='tri_layer_crawler',
        user='crawler_user',
        password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    cur.execute('SELECT id, url, title, content FROM crawled_items WHERE content IS NOT NULL')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="No documents found")
    
    item_ids = [r[0] for r in rows]
    urls = [r[1] for r in rows]
    titles = [r[2] for r in rows]
    documents = [r[3] for r in rows]
    
    # Find similar
    similar = TopicModeler.find_similar_documents(query, documents, top_k)
    
    results = []
    for idx, score in similar:
        results.append({
            'item_id': item_ids[idx],
            'url': urls[idx],
            'title': titles[idx],
            'similarity': round(score, 4)
        })
    
    return {'query': query, 'results': results}


@router.get("/trends")
async def get_topic_trends():
    """
    Get topic trends over time.
    """
    import psycopg2
    from src.analytics.topic_modeler import TopicModeler
    
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database='tri_layer_crawler',
        user='crawler_user',
        password='CrawlerPass2024!'
    )
    cur = conn.cursor()
    cur.execute('SELECT content, crawled_at FROM crawled_items WHERE content IS NOT NULL ORDER BY crawled_at')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if not rows:
        raise HTTPException(status_code=404, detail="No documents found")
    
    documents = [r[0] for r in rows]
    timestamps = [r[1] for r in rows]
    
    trends = TopicModeler.get_topic_trends(documents, timestamps)
    return trends
