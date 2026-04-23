"""
Simple FastAPI with working metrics.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
from fastapi.responses import Response
import random

app = FastAPI(title="Tri-Layer Crawler", version="5.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define metrics
crawler_urls_total = Counter('crawler_urls_total', 'Total URLs crawled', ['domain', 'status'])
crawler_queue_pending = Gauge('crawler_queue_pending', 'URLs pending in queue')
crawler_items_total = Gauge('crawler_items_total', 'Total items in database')
http_requests_total = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])

# Generate some test data
crawler_urls_total.labels(domain='books.toscrape.com', status='success').inc(25)
crawler_urls_total.labels(domain='quotes.toscrape.com', status='success').inc(15)
crawler_queue_pending.set(42)
crawler_items_total.set(40)

@app.get("/health")
async def health():
    http_requests_total.labels(method='GET', endpoint='/health', status='200').inc()
    return {"status": "healthy"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/v1/items")
async def get_items():
    http_requests_total.labels(method='GET', endpoint='/api/v1/items', status='200').inc()
    return {"items": [{"id": 1, "title": "Test Item"}]}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

