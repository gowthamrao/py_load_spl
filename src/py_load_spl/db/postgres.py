import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Literal

import psycopg2
from psycopg2.extensions import connection

from ..config import DatabaseSettings
from .base import DatabaseLoader

logger = logging.getLogger(__name__)

# This path assumes the script is run from the project root
SQL_SCHEMA_PATH = Path(__file__).parent / "sql/postgres_schema.sql"


class PostgresLoader(DatabaseLoader):
    """
    PostgreSQL-specific implementation of the DatabaseLoader.

    This adapter uses the `COPY` command for efficient bulk loading (F007.1).
    """

    def __init__(self, db_settings: DatabaseSettings) -> None:
        self.settings = db_settings
        self.conn: connection | None = None
        logger.info("Initialized PostgreSQL Loader.")

    @contextmanager
    def _get_conn(self) -> Generator[connection, None, None]:
        """Establish and manage a database connection."""
        if self.conn is None or self.conn.closed:
            try:
                logger.info(
                    f"Connecting to PostgreSQL at {self.settings.host}:{self.settings.port}..."
                )
                self.conn = psycopg2.connect(
                    dbname=self.settings.name,
                    user=self.settings.user,
                    password=self.settings.password,
                    host=self.settings.host,
                    port=self.settings.port,
                )
            except psycopg2.OperationalError as e:
                logger.error(f"Database connection failed: {e}")
                raise
        yield self.conn

    def initialize_schema(self) -> None:
        """Creates the necessary tables and structures from the DDL file."""
        logger.info("Initializing PostgreSQL schema...")
        if not SQL_SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found at {SQL_SCHEMA_PATH}")

        ddl = SQL_SCHEMA_PATH.read_text()

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
                conn.commit()
            logger.info("Schema initialization complete.")
        except psycopg2.Error as e:
            logger.error(f"Schema initialization failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def bulk_load_to_staging(self, intermediate_dir: Path) -> None:
        logger.info(f"Bulk loading data from {intermediate_dir} to staging tables...")
        # TODO: Use psycopg2's `copy_expert` to run the COPY command for each CSV.
        logger.info("Bulk load to staging complete.")

    def pre_load_optimization(self) -> None:
        logger.info(
            "Performing pre-load optimizations (dropping indexes/constraints)..."
        )
        # TODO: Execute DDL to drop indexes and FKs on production tables.
        logger.info("Pre-load optimizations complete.")

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        logger.info(f"Merging data from staging to production tables (mode: {mode})...")
        # TODO:
        # If 'full-load': TRUNCATE production tables and INSERT from staging.
        # If 'delta-load': Use INSERT ... ON CONFLICT (UPSERT) logic.
        # This must be done in a single transaction (F006.3).
        logger.info("Merge complete.")

    def post_load_cleanup(self) -> None:
        logger.info("Performing post-load cleanup (rebuilding indexes, analyzing)...")
        # TODO: Execute DDL to recreate indexes/FKs and run VACUUM ANALYZE.
        logger.info("Post-load cleanup complete.")

    def track_load_history(self, status: dict[str, Any]) -> None:
        logger.info(f"Tracking load history with status: {status}")
        # TODO: Insert a record into the `etl_load_history` table.
        logger.info("Load history updated.")
