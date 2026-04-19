# Save as tests/load/metrics_monitor.py
"""
Real-time metrics monitor during load tests.
"""

import time
import psutil
import requests
from typing import Dict


def get_cpu_usage() -> float:
    return psutil.cpu_percent(interval=1)


def get_memory_usage() -> Dict:
    mem = psutil.virtual_memory()
    return {
        'total_gb': round(mem.total / (1024**3), 2),
        'used_gb': round(mem.used / (1024**3), 2),
        'percent': mem.percent
    }


def get_queue_stats() -> Dict:
    try:
        import sys
        sys.path.insert(0, '.')
        from src.queue import RedisQueue
        q = RedisQueue()
        return q.get_stats()
    except Exception:
        return {}


def get_db_stats() -> Dict:
    try:
        import subprocess
        result = subprocess.run([
            'docker', 'exec', 'tri-layer-postgres',
            'psql', '-U', 'crawler_user', '-d', 'tri_layer_crawler',
            '-c', "SELECT COUNT(*) FROM crawled_items;"
        ], capture_output=True, text=True)
        return {'crawled_count': result.stdout.strip().split('\n')[2].strip()}
    except Exception:
        return {}


def monitor_during_test(duration_seconds: int = 60):
    """Monitor system during test execution."""
    print("\n📊 REAL-TIME MONITORING")
    print("="*50)
    
    for i in range(duration_seconds // 5):
        cpu = get_cpu_usage()
        mem = get_memory_usage()
        queue = get_queue_stats()
        db = get_db_stats()
        
        print(f"\n[{i*5}s] CPU: {cpu}% | MEM: {mem['percent']}% "
              f"| Queue Pending: {queue.get('pending', 'N/A')} "
              f"| DB Rows: {db.get('crawled_count', 'N/A')}")
        
        time.sleep(5)


if __name__ == "__main__":
    monitor_during_test(300)  # 5 minutes