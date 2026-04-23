"""
Schedule management API endpoints.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

router = APIRouter(prefix="/api/v1/schedules", tags=["schedules"])


class ScheduleInfo(BaseModel):
    name: str
    task: str
    schedule: str
    kwargs: dict
    enabled: bool


class CrawlRequest(BaseModel):
    urls: Optional[List[str]] = None
    priority: str = "normal"
    use_seeds: bool = False


@router.get("/")
async def get_schedules():
    """Get all scheduled tasks."""
    from src.workers import app
    
    schedules = []
    for name, schedule in app.conf.beat_schedule.items():
        schedule_str = str(schedule.get('schedule', 'unknown'))
        schedules.append({
            'name': name,
            'task': schedule.get('task', 'unknown'),
            'schedule': schedule_str,
            'kwargs': schedule.get('kwargs', {}),
            'enabled': True
        })
    
    return {
        'count': len(schedules),
        'schedules': schedules
    }


@router.post("/trigger/{schedule_name}")
async def trigger_schedule(schedule_name: str):
    """Manually trigger a scheduled task."""
    from src.workers import app
    
    if schedule_name not in app.conf.beat_schedule:
        raise HTTPException(status_code=404, detail=f"Schedule '{schedule_name}' not found")
    
    schedule = app.conf.beat_schedule[schedule_name]
    task_name = schedule['task']
    kwargs = schedule.get('kwargs', {})
    
    # Send task
    task = app.send_task(task_name, kwargs=kwargs)
    
    return {
        'status': 'triggered',
        'schedule': schedule_name,
        'task_id': task.id,
        'triggered_at': datetime.utcnow().isoformat()
    }


@router.post("/crawl/now")
async def crawl_now(request: CrawlRequest):
    """Trigger an immediate crawl."""
    from src.workers.periodic_tasks import scheduled_crawl, crawl_seed_list
    
    if request.use_seeds:
        task = crawl_seed_list.delay()
        return {
            'status': 'scheduled',
            'type': 'seed_crawl',
            'task_id': task.id
        }
    else:
        task = scheduled_crawl.delay(priority=request.priority)
        return {
            'status': 'scheduled',
            'type': f'{request.priority}_priority',
            'task_id': task.id
        }


@router.get("/next")
async def get_next_schedule():
    """Get information about next scheduled tasks."""
    try:
        import pickle
        from pathlib import Path
        
        schedule_file = Path('celerybeat-schedule')
        if schedule_file.exists():
            with open(schedule_file, 'rb') as f:
                schedule_data = pickle.load(f)
            
            next_tasks = []
            for name, entry in schedule_data.get('entries', {}).items():
                if hasattr(entry, 'last_run_at'):
                    next_tasks.append({
                        'name': name,
                        'last_run': entry.last_run_at.isoformat() if entry.last_run_at else None,
                        'total_runs': entry.total_run_count if hasattr(entry, 'total_run_count') else 0
                    })
            
            return {'next_tasks': next_tasks}
        
    except Exception as e:
        pass
    
    return {'message': 'Schedule information not available yet'}
