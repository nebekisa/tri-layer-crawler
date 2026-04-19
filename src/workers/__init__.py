"""
Celery application for distributed crawling.
"""

import os
from celery import Celery

# Redis connection with authentication
REDIS_PASSWORD = "RedisSecure2024!"
BROKER_URL = f"redis://:{REDIS_PASSWORD}@localhost:6379/0"
BACKEND_URL = f"redis://:{REDIS_PASSWORD}@localhost:6379/1"

app = Celery(
    'tri_layer_crawler',
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=['src.workers.crawl_tasks']
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
    broker_connection_retry=True,
    broker_connection_max_retries=10,
)

print(f'? Celery app configured with broker: redis://localhost:6379/0')
