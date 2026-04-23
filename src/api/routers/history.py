"""
Version history API endpoints.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

router = APIRouter(prefix="/api/v1/history", tags=["history"])


class VersionSummary(BaseModel):
    id: int
    version: int
    title: str
    content_length: int
    status_code: int
    title_changed: bool
    content_changed: bool
    meta_changed: bool
    crawled_at: Optional[str]
    recorded_at: Optional[str]


class VersionDetail(BaseModel):
    id: int
    item_id: int
    version: int
    title: str
    content: str
    meta_description: Optional[str]
    content_length: int
    status_code: int
    title_changed: bool
    content_changed: bool
    meta_changed: bool
    crawled_at: Optional[str]
    recorded_at: Optional[str]


@router.post("/backfill")
async def backfill_history():
    """Create initial versions for existing items."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    created = manager.backfill_from_items()
    
    return {
        "message": f"Created initial versions for {created} items",
        "created": created
    }


@router.get("/stats")
async def get_version_stats():
    """Get versioning statistics."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    return manager.get_stats()


@router.get("/item/{item_id}", response_model=List[VersionSummary])
async def get_item_history(item_id: int):
    """Get version history for an item."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    history = manager.get_history(item_id)
    
    if not history:
        raise HTTPException(status_code=404, detail=f"No history found for item {item_id}")
    
    return history


@router.get("/item/{item_id}/version/{version}", response_model=VersionDetail)
async def get_version(item_id: int, version: int):
    """Get a specific version of an item."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    snapshot = manager.get_version(item_id, version)
    
    if not snapshot:
        raise HTTPException(status_code=404, detail=f"Version {version} not found for item {item_id}")
    
    return snapshot


@router.get("/compare/{item_id}")
async def compare_versions(
    item_id: int,
    v1: int = Query(..., description="First version number"),
    v2: int = Query(..., description="Second version number")
):
    """Compare two versions of an item."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    comparison = manager.compare_versions(item_id, v1, v2)
    
    if 'error' in comparison:
        raise HTTPException(status_code=404, detail=comparison['error'])
    
    return comparison


@router.get("/changes")
async def get_changed_items(limit: int = Query(50, le=200)):
    """Get items that have changed between versions."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    return manager.get_changed_items(limit)


@router.get("/item/{item_id}/latest")
async def get_latest_version(item_id: int):
    """Get the latest version of an item."""
    from src.analytics.version_manager import VersionManager
    
    manager = VersionManager()
    history = manager.get_history(item_id)
    
    if not history:
        raise HTTPException(status_code=404, detail=f"No history found for item {item_id}")
    
    latest_version = history[0]['version']
    return manager.get_version(item_id, latest_version)
