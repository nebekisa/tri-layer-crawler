#!/usr/bin/env python
"""
Celery Beat Manager - Start scheduler and workers.
"""

import subprocess
import sys
import time
import argparse
from typing import List


def start_beat():
    """Start Celery Beat scheduler."""
    cmd = [
        "celery", "-A", "src.workers", "beat",
        "--loglevel=info",
    ]
    print("Starting Celery Beat scheduler...")
    return subprocess.Popen(cmd)


def start_worker(name: str = "worker1", concurrency: int = 2):
    """Start a Celery worker."""
    cmd = [
        "celery", "-A", "src.workers", "worker",
        "--loglevel=info",
        f"--concurrency={concurrency}",
        f"--hostname={name}@%h",
        "-P", "solo",
    ]
    print(f"Starting worker {name}...")
    return subprocess.Popen(cmd)


def start_all(workers: int = 2, concurrency: int = 2):
    """Start Beat scheduler and workers."""
    processes = []
    
    # Start Beat first
    processes.append(start_beat())
    time.sleep(3)
    
    # Start workers
    for i in range(workers):
        name = f"worker{i+1}"
        proc = start_worker(name, concurrency)
        processes.append(proc)
        time.sleep(2)
    
    print("\n? Scheduler and workers running!")
    print("   Beat scheduler: processing periodic tasks")
    print(f"   {workers} workers: executing crawl tasks")
    print("\nPress Ctrl+C to stop...")
    
    return processes


def main():
    parser = argparse.ArgumentParser(description='Celery Beat Manager')
    parser.add_argument('--beat-only', action='store_true', help='Start only beat scheduler')
    parser.add_argument('--workers', type=int, default=2, help='Number of workers')
    parser.add_argument('--concurrency', type=int, default=2, help='Worker concurrency')
    
    args = parser.parse_args()
    
    processes = []
    
    try:
        if args.beat_only:
            processes.append(start_beat())
        else:
            processes = start_all(args.workers, args.concurrency)
        
        for proc in processes:
            proc.wait()
            
    except KeyboardInterrupt:
        print("\nShutting down...")
        for proc in processes:
            proc.terminate()
            proc.wait()
        print("All processes stopped.")


if __name__ == "__main__":
    main()
