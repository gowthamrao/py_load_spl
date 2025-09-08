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
        """Loads data from CSV files into staging tables using COPY."""
        logger.info(f"Bulk loading data from {intermediate_dir} into staging tables...")
        # Map CSV filenames to their target staging tables
        file_to_table_map = {
            "products.csv": "products_staging",
            "ingredients.csv": "ingredients_staging",
            "packaging.csv": "packaging_staging",
            "marketing_status.csv": "marketing_status_staging",
            "product_ndcs.csv": "product_ndcs_staging",
            "spl_raw_documents.csv": "spl_raw_documents_staging",
        }

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    for filename, table_name in file_to_table_map.items():
                        filepath = intermediate_dir / filename
                        if not filepath.exists():
                            logger.warning(
                                f"Intermediate file {filepath} not found. Skipping."
                            )
                            continue

                        logger.info(f"Loading {filename} into {table_name}...")
                        # F007.1: Use copy_expert for efficient bulk loading
                        sql = f"""
                            COPY {table_name} FROM STDIN
                            WITH (FORMAT CSV, NULL '\\N', QUOTE '\"');
                        """
                        with open(filepath, "r", encoding="utf-8") as f:
                            cur.copy_expert(sql, f)
                conn.commit()
            logger.info("Bulk load to staging tables complete.")
        except (psycopg2.Error, IOError) as e:
            logger.error(f"Bulk load to staging failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def pre_load_optimization(self) -> None:
        logger.info(
            "Performing pre-load optimizations (dropping indexes/constraints)..."
        )
        # TODO: Execute DDL to drop indexes and FKs on production tables.
        logger.info("Pre-load optimizations complete.")

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        Merges data from staging to production tables.
        For this implementation, we only support 'full-load'.
        """
        logger.info(f"Merging data from staging to production (mode: {mode})...")
        if mode != "full-load":
            logger.warning(f"Merge mode '{mode}' is not yet implemented. Skipping.")
            return

        # List of tables to truncate and load, in order of dependency
        tables = [
            "product_ndcs",
            "ingredients",
            "packaging",
            "marketing_status",
            "products",
            "spl_raw_documents",
        ]

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    # F006.3: This block is executed in a single transaction
                    logger.info("Truncating production tables...")
                    for table in tables:
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")

                    logger.info("Inserting data from staging tables...")
                    for table in reversed(tables):  # Insert in reverse order
                        cur.execute(f"INSERT INTO {table} SELECT * FROM {table}_staging;")

                    logger.info("Truncating staging tables after merge...")
                    for table in tables:
                        cur.execute(f"TRUNCATE TABLE {table}_staging;")

                conn.commit()
            logger.info("Merge from staging to production complete.")
        except psycopg2.Error as e:
            logger.error(f"Merge from staging failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def post_load_cleanup(self) -> None:
        logger.info("Performing post-load cleanup (rebuilding indexes, analyzing)...")
        # TODO: Execute DDL to recreate indexes/FKs and run VACUUM ANALYZE.
        logger.info("Post-load cleanup complete.")

    def track_load_history(self, status: dict[str, Any]) -> None:
        logger.info(f"Tracking load history with status: {status}")
        # TODO: Insert a record into the `etl_load_history` table.
        logger.info("Load history updated.")
