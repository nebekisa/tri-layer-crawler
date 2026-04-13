"""
API routes for crawled items.
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.repositories.csv_repository import CsvRepository

router = APIRouter(prefix="/api/v1", tags=["items"])


class ItemResponse(BaseModel):
    """Response model for an item."""
    url: str
    title: str
    content: str
    meta_description: Optional[str] = ""
    timestamp: str
    status_code: int


@router.get("/items", response_model=List[ItemResponse])
async def get_items(limit: int = 100, offset: int = 0):
    """
    Get all crawled items with pagination.
    """
    repo = CsvRepository()
    items = repo.read_all()
    
    # Apply pagination
    paginated = items[offset:offset + limit]
    
    return paginated


@router.get("/items/count")
async def get_items_count():
    """
    Get total count of crawled items.
    """
    repo = CsvRepository()
    return {"count": repo.count()}


@router.get("/items/{url:path}")
async def get_item_by_url(url: str):
    """
    Get a specific item by its URL.
    """
    repo = CsvRepository()
    item = repo.find_by_url(url)
    
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    
    return item