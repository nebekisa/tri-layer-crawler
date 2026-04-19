"""
Summarization API endpoints.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/api/v1/summarize", tags=["summarization"])


class SummaryResponse(BaseModel):
    id: int
    item_id: int
    summary: str
    model: str
    generated_at: Optional[str]
    from_cache: bool = False


class SummarizeRequest(BaseModel):
    text: str
    max_length: Optional[int] = 150
    min_length: Optional[int] = 40


class DirectSummaryResponse(BaseModel):
    summary: str
    model: str
    input_length: int
    summary_length: int


@router.get("/item/{item_id}", response_model=SummaryResponse)
async def get_summary(item_id: int, refresh: bool = False):
    """
    Get summary for a crawled item.
    
    - If summary exists and refresh=False, returns cached summary
    - If refresh=True, regenerates summary
    """
    from src.analytics.summarizer import SummaryService
    
    service = SummaryService()
    
    if not refresh:
        # Check existing
        existing = service.get_summary(item_id)
        if existing:
            return SummaryResponse(**existing, from_cache=True)
    
    # Get item content
    from src.database.manager import DatabaseManager
    db = DatabaseManager()
    result = db.execute_query(
        "SELECT content FROM crawled_items WHERE id = %s",
        (item_id,)
    )
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
    
    content = result[0][0]
    
    # Generate new summary
    summary_data = service.generate_and_store(item_id, content)
    
    if not summary_data:
        raise HTTPException(status_code=500, detail="Failed to generate summary")
    
    return SummaryResponse(**summary_data, from_cache=False)


@router.post("/direct", response_model=DirectSummaryResponse)
async def summarize_text(request: SummarizeRequest):
    """
    Summarize arbitrary text (no storage).
    
    Useful for testing and one-off summarization.
    """
    from src.analytics.summarizer import Summarizer
    
    if len(request.text) < 100:
        raise HTTPException(
            status_code=400,
            detail="Text too short for summarization (minimum 100 characters)"
        )
    
    summary = Summarizer.summarize(
        request.text,
        max_length=request.max_length,
        min_length=request.min_length
    )
    
    if not summary:
        raise HTTPException(status_code=500, detail="Failed to generate summary")
    
    return DirectSummaryResponse(
        summary=summary,
        model=Summarizer.MODEL_NAME,
        input_length=len(request.text),
        summary_length=len(summary)
    )


@router.post("/generate-all")
async def generate_all_summaries(background_tasks: BackgroundTasks):
    """
    Generate summaries for all items that don't have one.
    
    Runs in background to avoid timeout.
    """
    from src.database.manager import DatabaseManager
    
    db = DatabaseManager()
    result = db.execute_query(
        """
        SELECT ci.id, ci.content
        FROM crawled_items ci
        LEFT JOIN content_summaries cs ON ci.id = cs.item_id
        WHERE cs.id IS NULL
        """
    )
    
    if not result:
        return {"message": "All items already have summaries", "count": 0}
    
    def generate_summaries():
        from src.analytics.summarizer import SummaryService
        service = SummaryService()
        
        generated = 0
        for item_id, content in result:
            if service.generate_and_store(item_id, content):
                generated += 1
        
        print(f"Generated {generated} summaries in background")
    
    background_tasks.add_task(generate_summaries)
    
    return {
        "message": f"Generating summaries for {len(result)} items in background",
        "count": len(result)
    }


@router.get("/model-info")
async def get_model_info():
    """Get information about the summarization model."""
    from src.analytics.summarizer import Summarizer
    return Summarizer.get_model_info()
