# Save as tests/load/scaling_test.py
"""
Worker scaling experiment.
"""

import sys
sys.path.insert(0, '.')

from tests.load.run_load_test import LoadTester

worker_configs = [1, 2, 4, 8, 16, 32]
results = []

print("\n📈 WORKER SCALING EXPERIMENT")
print("="*60)

for workers in worker_configs:
    print(f"\n>>> Testing with {workers} workers...")
    
    tester = LoadTester(
        test_id=f"scaling_w{workers}",
        max_workers=workers,
        rate_limit=0.1  # Minimal delay to test raw throughput
    )
    
    result = tester.run_full_test(url_count=200, max_crawl=200)
    results.append({
        'workers': workers,
        'throughput': result['crawl_metrics']['throughput_per_second'],
        'success_rate': result['crawl_metrics']['success_rate'],
        'avg_latency': result['crawl_metrics']['avg_response_ms']
    })

# Print scaling analysis
print("\n📊 SCALING ANALYSIS")
print("="*60)
print(f"{'Workers':<10} {'Throughput':<15} {'Success Rate':<15} {'Avg Latency':<15}")
print("-"*60)

for r in results:
    print(f"{r['workers']:<10} {r['throughput']:.2f} req/sec    {r['success_rate']:.1f}%           {r['avg_latency']:.0f}ms")

# Find optimal workers
optimal = max(results, key=lambda x: x['throughput'])
print(f"\n✅ Optimal workers: {optimal['workers']} (Throughput: {optimal['throughput']:.2f} req/sec)")

# Calculate efficiency
base_throughput = results[0]['throughput']
for r in results:
    r['efficiency'] = (r['throughput'] / base_throughput) / r['workers'] * 100
    print(f"   {r['workers']} workers: {r['efficiency']:.1f}% efficiency")