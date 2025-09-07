import logging
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    name: str = "spl_data"
    adapter: Literal["postgresql"] = "postgresql"


class Settings(BaseSettings):
    """Main application settings."""

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    log_level: str = "INFO"
    data_dir: str = "data"


def get_settings() -> Settings:
    """Get the application settings."""
    # In a real app, you might add logic here to load from different sources
    logger.info("Loading application settings...")
    return Settings()
