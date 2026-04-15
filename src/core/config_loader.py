"""
Configuration loader with environment variable support.
"""

import os
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, ValidationError


class CrawlerConfig(BaseModel):
    """Surface web crawler configuration."""
    name: str
    start_urls: List[str]
    user_agent: str
    download_delay: float
    concurrent_requests: int = 4
    request_timeout: int = 30
    max_retries: int = 3


class DeepCrawlerAuthConfig(BaseModel):
    """Authentication configuration for deep crawler."""
    enabled: bool = False
    login_url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    cookies_file: Optional[str] = None


class DeepCrawlerConfig(BaseModel):
    """Deep web crawler configuration (Playwright)."""
    enabled: bool = True
    browser_type: str = "chromium"
    headless: bool = True
    viewport_width: int = 1920
    viewport_height: int = 1080
    timeout: int = 30000
    wait_until: str = "networkidle"
    max_concurrent_browsers: int = 2
    screenshot_enabled: bool = True
    screenshot_quality: int = 85
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    js_enabled: bool = True
    wait_for_selector: Optional[str] = None
    scroll_to_bottom: bool = False
    scroll_pause: int = 2
    auth: DeepCrawlerAuthConfig = DeepCrawlerAuthConfig()


class StorageConfig(BaseModel):
    """Storage configuration."""
    csv_output_path: str
    database_path: str = "data/tri_layer.db"
    log_level: str


class APIConfig(BaseModel):
    """API configuration."""
    host: str
    port: int
    reload: bool


class DatabaseConfig(BaseModel):
    """Database configuration."""
    type: str = "sqlite"
    host: str = "localhost"
    port: int = 5432
    name: str = "tri_layer_crawler"
    user: str = "postgres"
    password: str = ""
    pool_size: int = 5
    max_overflow: int = 10
    
    @classmethod
    def from_env(cls, base_config: dict) -> dict:
        """Override config with environment variables."""
        env_mapping = {
            'DATABASE_TYPE': 'type',
            'DATABASE_HOST': 'host',
            'DATABASE_PORT': 'port',
            'DATABASE_NAME': 'name',
            'DATABASE_USER': 'user',
            'DATABASE_PASSWORD': 'password',
        }
        
        for env_var, config_key in env_mapping.items():
            if os.getenv(env_var):
                base_config[config_key] = os.getenv(env_var)
                if config_key == 'port':
                    base_config[config_key] = int(os.getenv(env_var))
        
        return base_config


class Settings(BaseModel):
    """Root configuration model."""
    crawler: CrawlerConfig
    deep_crawler: DeepCrawlerConfig = DeepCrawlerConfig()  # NEW
    storage: StorageConfig
    api: APIConfig
    database: DatabaseConfig = DatabaseConfig()


class ConfigurationError(Exception):
    """Configuration error."""
    pass


class ConfigLoader:
    """Configuration loader singleton."""
    
    _instance: Optional['Settings'] = None
    _config_path: Path = Path(__file__).parent.parent.parent / "config" / "settings.yaml"
    
    @classmethod
    def get_settings(cls) -> Settings:
        if cls._instance is None:
            cls._instance = cls._load_and_validate()
        return cls._instance
    
    @classmethod
    def _load_and_validate(cls) -> Settings:
        if not cls._config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {cls._config_path}")
        
        with open(cls._config_path, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # Apply environment variable overrides
        if 'database' in raw_config:
            raw_config['database'] = DatabaseConfig.from_env(raw_config['database'])
        
        # Ensure deep_crawler section exists
        if 'deep_crawler' not in raw_config:
            raw_config['deep_crawler'] = {}
        
        try:
            return Settings(**raw_config)
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e.errors()}")
    
    @classmethod
    def reload(cls) -> Settings:
        cls._instance = None
        return cls.get_settings()


def get_settings() -> Settings:
    """Get application settings."""
    return ConfigLoader.get_settings()