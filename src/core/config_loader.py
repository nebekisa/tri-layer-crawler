"""
Configuration Loader Module

This module provides a singleton configuration object loaded from settings.yaml.
It ensures that the configuration is loaded only once and shared across all modules.

Architecture Note:
    We use Pydantic for validation. This ensures that if a required key is missing
    or has the wrong type, the application fails FAST at startup rather than
    mysteriously during runtime.
"""

import os
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, ValidationError

# -----------------------------------------------------------------------------
# Pydantic Models for Type Safety and Validation
# -----------------------------------------------------------------------------

class CrawlerConfig(BaseModel):
    """Configuration specific to the crawler."""
    name: str
    start_urls: List[str]
    user_agent: str
    download_delay: float
    concurrent_requests: int = 4      # NEW with default
    request_timeout: int = 30         # NEW with default
    max_retries: int = 3              # NEW with default
class DatabaseConfig(BaseModel):
    """Configuration for database connection."""
    type: str = "sqlite"  # sqlite or postgresql
    host: str = "localhost"
    port: int = 5433
    name: str = "tri_layer_crawler"
    user: str = "postgres"
    password: str = ""
    pool_size: int = 5
    max_overflow: int = 10

class StorageConfig(BaseModel):
    """Configuration for data persistence."""
    csv_output_path: str
    database_path: str = "data/tri_layer.db"
    log_level: str

class APIConfig(BaseModel):
    """Configuration for the FastAPI server."""
    host: str
    port: int
    reload: bool

class Settings(BaseModel):
    """Root configuration model."""
    crawler: CrawlerConfig
    storage: StorageConfig
    api: APIConfig
    database: DatabaseConfig = DatabaseConfig()  # NEW

# -----------------------------------------------------------------------------
# Configuration Loader Singleton
# -----------------------------------------------------------------------------

class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass

class ConfigLoader:
    """
    Singleton loader for application configuration.
    
    Usage:
        settings = ConfigLoader.get_settings()
        print(settings.crawler.start_urls)
    """
    
    _instance: Optional[Settings] = None
    _config_path: Path = Path(__file__).parent.parent.parent / "config" / "settings.yaml"
    
    @classmethod
    def get_settings(cls) -> Settings:
        """
        Returns the cached settings object. Loads from disk on first call.
        
        Returns:
            Settings: Validated Pydantic settings object.
            
        Raises:
            ConfigurationError: If the YAML file is missing or invalid.
        """
        if cls._instance is None:
            cls._instance = cls._load_and_validate()
        return cls._instance
    
    @classmethod
    def _load_and_validate(cls) -> Settings:
        """
        Loads YAML from disk and validates against Pydantic models.
        
        Why this is critical:
            1. Centralized path resolution: Path is relative to THIS file, not the caller.
            2. Early validation: Catches typos in settings.yaml immediately.
        """
        if not cls._config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found at: {cls._config_path}\n"
                f"Please ensure 'config/settings.yaml' exists."
            )
        
        try:
            with open(cls._config_path, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in settings.yaml: {e}")
        
        try:
            # Pydantic will throw a detailed ValidationError if structure is wrong
            validated_config = Settings(**raw_config)
            return validated_config
        except ValidationError as e:
            raise ConfigurationError(
                f"Configuration validation failed. Check settings.yaml structure.\n"
                f"Errors: {e.errors()}"
            )
    
    @classmethod
    def reload(cls) -> Settings:
        """
        Force a reload of configuration from disk.
        Useful for testing or when config changes at runtime (rare).
        """
        cls._instance = None
        return cls.get_settings()


# -----------------------------------------------------------------------------
# Convenience Function (Use this 90% of the time)
# -----------------------------------------------------------------------------

def get_settings() -> Settings:
    """
    Functional shortcut to access configuration.
    
    Example:
        from src.core.config_loader import get_settings
        
        settings = get_settings()
        csv_path = settings.storage.csv_output_path
    """
    return ConfigLoader.get_settings()