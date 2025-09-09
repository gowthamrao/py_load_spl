import logging
import os
from typing import Literal

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


from typing import Optional


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(env_prefix="DB_")

    adapter: Literal["postgresql", "sqlite"] = "postgresql"
    # For SQLite, 'name' is the file path. For Postgres, it's the database name.
    name: str = "spl_data"
    # The following are optional, mainly for Postgres
    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    user: Optional[str] = "postgres"
    password: Optional[str] = "postgres"
    optimize_full_load: bool = Field(
        default=True,
        description="Enable dropping/recreating indexes and FKs during a full load.",
        env="DB_OPTIMIZE_FULL_LOAD",
    )


class Settings(BaseSettings):
    """Main application settings."""

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    log_level: str = "INFO"
    data_dir: str = "data"
    # The FRD requires a configurable download source (F001.1)
    fda_source_url: HttpUrl = (
        "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"  # type: ignore
    )
    download_path: str = "data/downloads"
    quarantine_path: str = Field(
        default="data/quarantine",
        description="Directory to move corrupted or unparseable XML files.",
        env="QUARANTINE_PATH",
    )
    max_workers: int | None = Field(
        default_factory=os.cpu_count,
        description="Number of parallel processes for parsing. Defaults to number of CPUs.",
        env="MAX_WORKERS",
    )
    intermediate_format: Literal["csv", "parquet"] = Field(
        default="csv",
        description="The file format for intermediate data files.",
        env="INTERMEDIATE_FORMAT",
    )


def get_settings() -> Settings:
    """Get the application settings."""
    # In a real app, you might add logic here to load from different sources
    logger.info("Loading application settings...")
    return Settings()
