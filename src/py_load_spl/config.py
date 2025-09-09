import logging
import os

from pydantic import Field, HttpUrl
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
    adapter: str = "postgresql"


class Settings(BaseSettings):
    """Main application settings."""

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    log_level: str = "INFO"
    data_dir: str = "data"
    # The FRD requires a configurable download source (F001.1)
    fda_source_url: HttpUrl = "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"  # type: ignore
    download_path: str = "data/downloads"
    max_workers: int | None = Field(
        default_factory=os.cpu_count,
        description="Number of parallel processes for parsing. Defaults to number of CPUs.",
        env="MAX_WORKERS",
    )


def get_settings() -> Settings:
    """Get the application settings."""
    # In a real app, you might add logic here to load from different sources
    logger.info("Loading application settings...")
    return Settings()
