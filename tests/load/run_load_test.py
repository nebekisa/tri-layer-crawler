"""
Production Load Testing Suite for Tri-Layer Crawler.
Fixed version - WORKING.
"""

import sys
import time
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.queue.redis_queue import RedisQueue
from src.crawlers.concurrent_crawler import ConcurrentCrawler


class LoadTester:
    """Production load testing harness."""
    
    def __init__(self):
        self.queue = RedisQueue()
        self.results = []
    
    def generate_test_urls(self, count: int) -> List[str]:
        """Generate test URLs that respond quickly."""
        urls = []
        for i in range(count):
            # Use httpbin's simple endpoints for fast testing
            urls.append(f"https://httpbin.org/status/200?test={i}")
        return urls
    
    def run_test(self, url_count: int, workers: int = 4, max_crawl: int = None):
        """Execute load test."""
        print(f"\n{'='*60}")
        print(f"?? LOAD TEST: {url_count} URLs, {workers} workers")
        print(f"{'='*60}\n")
        
        # Generate URLs
        urls = self.generate_test_urls(url_count)
        print(f"?? Generated {len(urls)} test URLs")
        
        # Clear queue and add URLs
        self.queue.clear()
        for url in urls:
            self.queue.push(url)
        print(f"?? Queue loaded: {len(urls)} URLs pending")
        
        # Configure crawler
        crawler = ConcurrentCrawler()
        crawler.max_workers = workers
        crawler.download_delay = 0.1  # Minimal delay for testing
        
        # Run test
        print(f"??? Starting crawl...")
        start_time = time.time()
        
        succeeded = 0
        failed = 0
        response_times = []
        batch_size = 50
        
        # Collect URLs from queue
        crawl_urls = []
        limit = max_crawl or url_count
        for _ in range(limit):
            item = self.queue.pop(timeout=1)
            if item:
                crawl_urls.append(item['url'])
        
        print(f"?? Crawling {len(crawl_urls)} URLs...")
        
        # Crawl with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for url in crawl_urls:
                future = executor.submit(self._crawl_single_with_timing, crawler, url)
                futures[future] = url
            
            completed = 0
            for future in as_completed(futures):
                completed += 1
                url = futures[future]
                
                try:
                    success, resp_time = future.result()
                    response_times.append(resp_time)
                    if success:
                        succeeded += 1
                        self.queue.mark_complete(url, success=True)
                    else:
                        failed += 1
                        self.queue.mark_complete(url, success=False)
                except Exception as e:
                    failed += 1
                    response_times.append(0)
                
                if completed % 50 == 0:
                    print(f"   Progress: {completed}/{len(crawl_urls)} URLs")
        
        duration = time.time() - start_time
        
        # Results
        print(f"\n{'='*60}")
        print(f"?? RESULTS")
        print(f"{'='*60}")
        
        total = succeeded + failed
        throughput = total / duration if duration > 0 else 0
        success_rate = (succeeded / total * 100) if total > 0 else 0
        avg_latency = sum(response_times) / len(response_times) if response_times else 0
        
        print(f"""
Duration:       {duration:.2f}s
Throughput:     {throughput:.2f} req/sec ({throughput*3600:.0f} req/hour)
Success Rate:   {success_rate:.1f}%
Succeeded:      {succeeded}
Failed:         {failed}
Avg Latency:    {avg_latency:.2f}ms
        """)
        
        return {
            'url_count': url_count,
            'crawled': total,
            'workers': workers,
            'duration': duration,
            'succeeded': succeeded,
            'failed': failed,
            'throughput': throughput,
            'success_rate': success_rate,
            'avg_latency_ms': avg_latency
        }
    
    def _crawl_single_with_timing(self, crawler, url: str):
        """Crawl single URL with timing."""
        req_start = time.time()
        try:
            result = crawler._crawl_single_url(url)
            req_time = (time.time() - req_start) * 1000
            return (result is not None, req_time)
        except Exception:
            req_time = (time.time() - req_start) * 1000
            return (False, req_time)
    
    def run_scaling_test(self):
        """Test different worker counts."""
        print("\n" + "="*60)
        print("?? SCALING TEST: 4 vs 8 vs 16 workers")
        print("="*60)
        
        results = []
        test_size = 100  # Smaller size for faster testing
        
        for workers in [4, 8, 16]:
            print(f"\n>>> Testing {workers} workers...")
            result = self.run_test(url_count=test_size, workers=workers, max_crawl=test_size)
            results.append(result)
            time.sleep(3)  # Cool down
        
        # Comparison
        print("\n" + "="*60)
        print("?? SCALING COMPARISON")
        print("="*60)
        print(f"{'Workers':<10} {'Throughput':<15} {'Latency':<15} {'Success':<15}")
        print("-"*60)
        for r in results:
            print(f"{r['workers']:<10} {r['throughput']:<15.2f} {r['avg_latency_ms']:<15.2f} {r['success_rate']:<15.1f}%")
        
        # Find optimal
        if results:
            best = max(results, key=lambda x: x['throughput'])
            print(f"\n? Optimal workers: {best['workers']} (Throughput: {best['throughput']:.2f} req/sec)")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='Tri-Layer Crawler Load Tester')
    parser.add_argument('--size', type=int, default=100, help='Number of URLs to test')
    parser.add_argument('--workers', type=int, default=4, help='Number of workers')
    parser.add_argument('--scale', action='store_true', help='Run scaling test')
    parser.add_argument('--max', type=int, help='Maximum URLs to crawl')
    
    args = parser.parse_args()
    
    tester = LoadTester()
    
    if args.scale:
        results = tester.run_scaling_test()
        
        # Save results
        output_file = Path("tests/load/results") / f"scaling_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\n?? Results saved: {output_file}")
    else:
        result = tester.run_test(args.size, args.workers, args.max)
        
        # Save result
        output_file = Path("tests/load/results") / f"test_{args.size}_{args.workers}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n?? Result saved: {output_file}")


if __name__ == "__main__":
    main()
