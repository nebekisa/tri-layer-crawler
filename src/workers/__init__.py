"""
Celery application with beat scheduler.
"""

import os
from celery import Celery
from celery.schedules import crontab
from datetime import timedelta

# Redis connection
REDIS_PASSWORD = "RedisSecure2024!"
BROKER_URL = f"redis://:{REDIS_PASSWORD}@localhost:6379/0"
BACKEND_URL = f"redis://:{REDIS_PASSWORD}@localhost:6379/1"

app = Celery(
    'tri_layer_crawler',
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=['src.workers.crawl_tasks', 'src.workers.periodic_tasks']
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    
    # Beat scheduler settings
    beat_schedule_filename='celerybeat-schedule',
    beat_max_loop_interval=60,
)

# Periodic task schedule
app.conf.beat_schedule = {
    # Health check every 30 minutes
    'health-check': {
        'task': 'health_check',
        'schedule': timedelta(minutes=30),
        'options': {'queue': 'default'},
    },
    
    # Process queue every hour
    'process-queue-hourly': {
        'task': 'process_queue',
        'schedule': timedelta(hours=1),
        'kwargs': {'max_tasks': 100},
        'options': {'queue': 'default'},
    },
    
    # Daily full crawl at 2 AM
    'daily-crawl': {
        'task': 'scheduled_crawl',
        'schedule': crontab(hour=2, minute=0),
        'kwargs': {'priority': 'normal'},
        'options': {'queue': 'default'},
    },
    
    # Priority crawl every 6 hours
    'priority-crawl': {
        'task': 'scheduled_crawl',
        'schedule': timedelta(hours=6),
        'kwargs': {'priority': 'high'},
        'options': {'queue': 'high_priority'},
    },
    
    # Weekly deep crawl (Sunday 3 AM)
    'weekly-deep-crawl': {
        'task': 'scheduled_crawl',
        'schedule': crontab(hour=3, minute=0, day_of_week=0),
        'kwargs': {'priority': 'deep', 'max_depth': 3},
        'options': {'queue': 'default'},
    },
}

print(f'? Celery app configured with beat scheduler')
