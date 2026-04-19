"""
FastAPI application with health checks and monitoring.
"""

import os
import time
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers import items
from src.core.config_loader import get_settings
from src.core.logging_config import setup_logging
from src.database.manager import get_db_manager
from sqlalchemy import text
from src.monitoring import setup_metrics
from src.api.routers import search


# Setup logging
settings = get_settings()
setup_logging(
    log_level=settings.storage.log_level,
    log_file="logs/api.log",
    json_format=os.getenv("JSON_LOGS", "false").lower() == "true"
)

# Track startup time
START_TIME = time.time()


def create_app() -> FastAPI:
    """Application factory."""
    app = FastAPI(
        title="Tri-Layer Intelligence Crawler API",
        description="REST API for accessing crawled web data",
        version="1.0.0",
    )
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    setup_metrics(app)
    
    # Register routers
    app.include_router(items.router)
    app.include_router(search.router)
app.include_router(summarize.router)
app.include_router(topics.router)
app.include_router(tasks.router)
    
    # Health check endpoints
    @app.get("/health", tags=["monitoring"])
    async def health_check():
        """
        Basic health check for load balancers.
        Returns 200 if service is running.
        """
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat() + "Z"}
    
    @app.get("/health/tor")
    async def tor_health():
        """Check Tor connection status."""
        try:
            from src.tor.tor_manager import TorManager
            t = TorManager()
            return {"tor_connected": t.verify_connection()}
        except Exception as e:
            return {"tor_connected": False, "error": str(e)}
    
    @app.get("/health/ready", tags=["monitoring"])
    async def readiness_check():
        """
        Readiness probe for Kubernetes.
        Checks if service can accept requests.
        """
        # Check database connection
        try:
            from sqlalchemy import text
            db_manager = get_db_manager()
            session = db_manager.get_session()
            session.execute(text("SELECT 1"))  # FIX: Wrap in text()
            session.close()
            db_status = "connected"
        except Exception as e:
            db_status = f"error: {str(e)}"
            return {"status": "not ready", "database": db_status}
        
        return {
            "status": "ready",
            "database": db_status,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
    
    @app.get("/health/live", tags=["monitoring"])
    async def liveness_check():
        """
        Liveness probe for Kubernetes.
        Simple check that service is alive.
        """
        return {"status": "alive", "timestamp": datetime.utcnow().isoformat() + "Z"}
    
    @app.get("/metrics", tags=["monitoring"])
    async def get_metrics():
        """
        Application metrics endpoint.
        Prometheus-compatible format.
        """
        # Collect metrics with error handling
        item_count = 0
        db_status = "disconnected"
        
        try:
            from src.repositories.db_repository import DatabaseRepository
            repo = DatabaseRepository()
            item_count = repo.count()
            repo.close()
            db_status = "connected"
        except Exception as e:
            db_status = f"error: {str(e)}"
        
        uptime = time.time() - START_TIME
        
        # Prometheus text format
        metrics = [
            "# HELP crawler_items_total Total number of crawled items",
            "# TYPE crawler_items_total gauge",
            f"crawler_items_total {item_count}",
            "",
            "# HELP app_uptime_seconds Application uptime in seconds",
            "# TYPE app_uptime_seconds gauge",
            f"app_uptime_seconds {uptime:.2f}",
            "",
            "# HELP app_info Application information",
            "# TYPE app_info gauge",
            f'app_info{{version="1.0.0",python="3.11",db_status="{db_status}"}} 1',
        ]
        
        return "\n".join(metrics)
    
    @app.get("/", tags=["default"])
    async def root():
        return {
            "name": "Tri-Layer Intelligence Crawler API",
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/health",
            "metrics": "/metrics",
            "endpoints": {
                "items": "/api/v1/items",
                "items_count": "/api/v1/items/count",
                "items_by_domain": "/api/v1/items/domain/{domain}",
                "search": "/api/v1/search"
            }
        }
    
    return app


app = create_app()