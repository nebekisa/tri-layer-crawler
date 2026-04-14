"""
API routes for crawled items (Database version).
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.repositories.db_repository import DatabaseRepository

router = APIRouter(prefix="/api/v1", tags=["items"])


class ItemResponse(BaseModel):
    """Response model for an item."""
    id: int
    url: str
    title: str
    content: str
    meta_description: str = ""
    domain: str
    status_code: int
    content_length: int
    crawled_at: str


@router.get("/items", response_model=List[ItemResponse])
async def get_items(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get all crawled items with pagination."""
    repo = DatabaseRepository()
    try:
        items = repo.get_all(limit=limit, offset=offset)
        return [item.to_dict() for item in items]
    finally:
        repo.close()


@router.get("/items/count")
async def get_items_count():
    """Get total count of crawled items."""
    repo = DatabaseRepository()
    try:
        return {"count": repo.count()}
    finally:
        repo.close()


@router.get("/items/domain/{domain}")
async def get_items_by_domain(domain: str):
    """Get all items from a specific domain."""
    repo = DatabaseRepository()
    try:
        items = repo.get_by_domain(domain)
        return [item.to_dict() for item in items]
    finally:
        repo.close()


@router.get("/items/{url:path}")
async def get_item_by_url(url: str):
    """Get a specific item by its URL."""
    repo = DatabaseRepository()
    try:
        item = repo.get_by_url(url)
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
        return item.to_dict()
    finally:
        repo.close()


@router.delete("/items/{url:path}")
async def delete_item(url: str):
    """Delete an item by URL."""
    repo = DatabaseRepository()
    try:
        deleted = repo.delete_by_url(url)
        if not deleted:
            raise HTTPException(status_code=404, detail="Item not found")
        return {"status": "deleted", "url": url}
    finally:
        repo.close()