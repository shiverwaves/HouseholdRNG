"""
API Configuration

Manages environment-based configuration for the API server.
"""

import os
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    Settings can be overridden with environment variables or .env file.
    """
    
    # API Settings
    app_name: str = "Household Generation API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Database
    database_url: str
    
    # CORS Settings
    cors_origins: list[str] = ["http://localhost:3000", "http://localhost:8080"]
    
    # Rate Limiting
    rate_limit_enabled: bool = True
    rate_limit_per_minute: int = 60
    
    # Generation Limits
    max_households_per_request: int = 100
    default_state: str = "HI"
    default_year: int = 2023
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Using lru_cache ensures settings are loaded once and reused.
    """
    return Settings()