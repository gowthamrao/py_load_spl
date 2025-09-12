import logging
import os
from typing import Annotated, Literal

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


# Base class for all database settings
class BaseDBSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_", extra="allow")
    optimize_full_load: bool = Field(
        default=True,
        description="Enable dropping/recreating indexes and FKs during a full load.",
        validation_alias="DB_OPTIMIZE_FULL_LOAD",
    )


class PostgresSettings(BaseDBSettings):
    adapter: Literal["postgresql"] = "postgresql"
    name: str = "spl_data"
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"


class SqliteSettings(BaseDBSettings):
    adapter: Literal["sqlite"] = "sqlite"
    name: str = "spl_data.db"  # For SQLite, 'name' is the file path


class RedshiftSettings(BaseDBSettings):
    adapter: Literal["redshift"] = "redshift"
    name: str = "spl_data"
    host: str
    port: int = 5439
    user: str
    password: str
    iam_role_arn: str


class DatabricksSettings(BaseDBSettings):
    adapter: Literal["databricks"] = "databricks"
    server_hostname: str
    http_path: str
    token: str
    s3_staging_path: str  # e.g. s3://my-bucket/staging


DatabaseSettings = Annotated[
    PostgresSettings | SqliteSettings | RedshiftSettings | DatabricksSettings,
    Field(discriminator="adapter"),
]


class S3Settings(BaseSettings):
    """Settings for AWS S3, used for cloud-based loaders."""

    model_config = SettingsConfigDict(env_prefix="S3_")
    bucket: str | None = None
    prefix: str = "spl_data"


class Settings(BaseSettings):
    """Main application settings."""

    model_config = SettingsConfigDict(extra="allow")
    db: DatabaseSettings = Field(default_factory=PostgresSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    log_level: str = "INFO"
    data_dir: str = "data"
    fda_source_url: HttpUrl = (
        "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"  # type: ignore
    )
    download_path: str = "data/downloads"
    quarantine_path: str = Field(
        default="data/quarantine",
        description="Directory to move corrupted or unparseable XML files.",
        validation_alias="QUARANTINE_PATH",
    )
    max_workers: int | None = Field(
        default_factory=os.cpu_count,
        description="Number of parallel processes for parsing. Defaults to number of CPUs.",
        validation_alias="MAX_WORKERS",
    )
    intermediate_format: Literal["csv", "parquet"] = Field(
        default="csv",
        description="The file format for intermediate data files.",
        validation_alias="INTERMEDIATE_FORMAT",
    )


def get_settings() -> Settings:
    """Get the application settings."""
    logger.info("Loading application settings...")
    # Pydantic-settings automatically loads from environment variables,
    # which is what we want.
    return Settings()
