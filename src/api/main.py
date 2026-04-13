"""
FastAPI application entry point.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers import items
from src.core.config_loader import get_settings

settings = get_settings()


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
    
    # Register routers
    app.include_router(items.router)
    
    @app.get("/")
    async def root():
        return {
            "name": "Tri-Layer Intelligence Crawler API",
            "version": "1.0.0",
            "docs": "/docs",
            "endpoints": {
                "items": "/api/v1/items",
                "items_count": "/api/v1/items/count",
            }
        }
    
    return app


app = create_app()