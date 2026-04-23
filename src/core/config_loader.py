"""
Configuration Loader Module

This module provides a singleton configuration object loaded from settings.yaml.
"""

import os
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, ValidationError


class CrawlerConfig(BaseModel):
    """Configuration specific to the Scrapy crawler."""
    name: str
    start_urls: List[str]
    user_agent: str
    download_delay: float
    concurrent_requests: int
    request_timeout: int = 30
    max_retries: int = 3



class DatabaseConfig(BaseModel):
    """Configuration for database connection."""
    host: str = "localhost"
    port: int = 5432
    name: str = "tri_layer_crawler"
    user: str = "crawler_user"
    password: str = "CrawlerPass2024!"
    pool_size: int = 10
    echo: bool = False


class StorageConfig(BaseModel):
    """Configuration for data persistence."""
    csv_output_path: str
    log_level: str


class APIConfig(BaseModel):
    """Configuration for the FastAPI server."""
    host: str
    port: int
    reload: bool


class Settings(BaseModel):
    """Root configuration model aggregating all sub-configs."""
    crawler: CrawlerConfig
    storage: StorageConfig
    api: APIConfig
    database: DatabaseConfig = DatabaseConfig()


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


class ConfigLoader:
    """Singleton loader for application configuration."""
    
    _instance: Optional[Settings] = None
    _config_path: Path = Path(__file__).parent.parent.parent / "config" / "settings.yaml"
    
    @classmethod
    def get_settings(cls) -> Settings:
        """Returns the cached settings object."""
        if cls._instance is None:
            cls._instance = cls._load_and_validate()
        return cls._instance
    
    @classmethod
    def _load_and_validate(cls) -> Settings:
        """Loads YAML from disk and validates against Pydantic models."""
        if not cls._config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found at: {cls._config_path}"
            )
        
        try:
            with open(cls._config_path, 'r', encoding='utf-8') as f:
                raw_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in settings.yaml: {e}")
        
        try:
            validated_config = Settings(**raw_config)
            return validated_config
        except ValidationError as e:
            raise ConfigurationError(f"Config validation failed: {e}")
    
    @classmethod
    def reload(cls) -> Settings:
        """Force a reload of configuration from disk."""
        cls._instance = None
        return cls.get_settings()


def get_settings() -> Settings:
    """Functional shortcut to access configuration."""
    return ConfigLoader.get_settings()
