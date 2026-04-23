from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(title="Tri-Layer Intelligence Crawler", version="5.0.0")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/metrics")
async def get_metrics():
    try:
        from src.monitoring.metrics import get_registry, MetricsCollector
        MetricsCollector.update_all()
        registry = get_registry()
    except:
        from prometheus_client import REGISTRY
        registry = REGISTRY
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

# Import routers
from src.api.routers import items, analytics
app.include_router(items.router)
app.include_router(analytics.router)

print("? Analytics router loaded!")
