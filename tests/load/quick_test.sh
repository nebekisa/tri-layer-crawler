#!/bin/bash
# Quick Load Test - Bash version (use Git Bash or WSL)
# Or use PowerShell equivalent below

echo "=== QUICK LOAD TEST ==="

# Test 100 URLs
python -c "
import sys, time
sys.path.insert(0, '.')
from src.queue import RedisQueue
from src.crawlers.concurrent_crawler import ConcurrentCrawler

# Generate 100 test URLs
urls = [f'https://httpbin.org/status/200?id={i}' for i in range(100)]

# Queue them
q = RedisQueue()
q.clear()
q.push_batch(urls)

# Crawl with timing
crawler = ConcurrentCrawler()
crawler.max_workers = 4

start = time.time()
results = crawler.crawl_surface_only(urls[:100])
elapsed = time.time() - start

print(f'\n?? RESULTS:')
print(f'   URLs: {len(results)}/100')
print(f'   Time: {elapsed:.2f}s')
print(f'   Rate: {100/elapsed:.2f} req/sec')
print(f'   Success: {len(results)/100*100:.1f}%')
"
