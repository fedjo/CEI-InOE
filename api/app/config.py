"""Application configuration from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API configuration settings."""
    
    # Database
    database_url: str = "postgresql://ocei:ocei@postgres:5432/ocei3"
    
    # CORS
    cors_origins: str = "http://localhost:3000,http://localhost:5173"

    # Authentication
    api_key: str = "cei-inoe-dev-key-2026"
    
    # API
    api_title: str = "CEI-InOE Data API"
    api_version: str = "1.0.0"
    
    # Pagination defaults
    default_page_size: int = 100
    max_page_size: int = 1000
    
    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()


def get_cors_origins() -> list[str]:
    """Parse CORS origins from comma-separated string."""
    return [origin.strip() for origin in settings.cors_origins.split(",") if origin.strip()]
