"""
Celery task API endpoints.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])


class CrawlTaskRequest(BaseModel):
    url: str
    priority: Optional[int] = 0


class BatchCrawlRequest(BaseModel):
    urls: List[str]


class TaskResponse(BaseModel):
    task_id: str
    status: str
    url: str


@router.post("/crawl", response_model=TaskResponse)
async def submit_crawl_task(request: CrawlTaskRequest):
    """
    Submit a URL for crawling via Celery.
    """
    from src.workers.crawl_tasks import crawl_url, crawl_high_priority
    
    if request.priority >= 1:
        task = crawl_high_priority.delay(request.url)
    else:
        task = crawl_url.delay(request.url, priority=request.priority)
    
    return TaskResponse(
        task_id=task.id,
        status="submitted",
        url=request.url
    )


@router.post("/batch")
async def submit_batch_crawl(request: BatchCrawlRequest):
    """
    Submit multiple URLs for batch crawling.
    """
    from src.workers.crawl_tasks import crawl_batch
    
    task = crawl_batch.delay(request.urls)
    
    return {
        "task_id": task.id,
        "status": "submitted",
        "url_count": len(request.urls)
    }


@router.get("/{task_id}")
async def get_task_status(task_id: str):
    """
    Get status of a Celery task.
    """
    from celery.result import AsyncResult
    from src.workers import app
    
    result = AsyncResult(task_id, app=app)
    
    response = {
        "task_id": task_id,
        "status": result.state,
    }
    
    if result.ready():
        if result.successful():
            response["result"] = result.result
        else:
            response["error"] = str(result.info)
    
    return response


@router.post("/queue/process")
async def process_queue(background_tasks: BackgroundTasks, max_tasks: int = 10):
    """
    Process URLs from Redis queue.
    """
    from src.workers.crawl_tasks import process_queue
    
    task = process_queue.delay(max_tasks)
    
    return {
        "task_id": task.id,
        "message": f"Processing up to {max_tasks} URLs from queue"
    }


@router.get("/stats/workers")
async def get_worker_stats():
    """
    Get Celery worker statistics.
    """
    from src.workers import app
    
    inspect = app.control.inspect()
    
    stats = inspect.stats()
    active = inspect.active()
    scheduled = inspect.scheduled()
    registered = inspect.registered()
    
    workers = []
    
    if stats:
        for worker_name, worker_stats in stats.items():
            workers.append({
                "name": worker_name,
                "pool_size": worker_stats.get('pool', {}).get('max-concurrency', 0),
                "processed": worker_stats.get('total', {}).get('processed', 0),
                "active_tasks": len(active.get(worker_name, [])) if active else 0,
                "scheduled_tasks": len(scheduled.get(worker_name, [])) if scheduled else 0,
            })
    
    return {
        "worker_count": len(workers),
        "workers": workers,
        "registered_tasks": list(registered.values())[0] if registered else []
    }
