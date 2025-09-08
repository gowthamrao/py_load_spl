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
        logger.info("Performing pre-load optimizations...")
        # In a real-world scenario for very large loads, we would drop indexes
        # and foreign keys here to speed up the data insertion.
        # For this implementation, we will just log the action.
        logger.info("Pre-load optimization step complete (simulation).")

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        Merges data from staging to production tables.
        For 'full-load', it truncates production tables and inserts from staging.
        """
        logger.info(f"Merging data from staging to production (mode: {mode})...")
        if mode != "full-load":
            logger.warning(f"Merge mode '{mode}' is not yet implemented. Skipping.")
            return

        # The order is important to respect foreign key constraints.
        # Truncate from child to parent, insert from parent to child.
        tables_in_dependency_order = [
            "spl_raw_documents",
            "products",
            "product_ndcs",
            "ingredients",
            "packaging",
            "marketing_status",
        ]

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    # This block is executed in a single transaction (F006.3)
                    logger.info("Disabling triggers for session...")
                    cur.execute("SET session_replication_role = 'replica';")

                    logger.info("Truncating production tables...")
                    for table in reversed(tables_in_dependency_order):
                        logger.debug(f"Truncating {table}...")
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")

                    logger.info("Inserting data from staging into production tables...")
                    for table in tables_in_dependency_order:
                        logger.debug(f"Loading data into {table}...")
                        cur.execute(f"INSERT INTO {table} SELECT * FROM {table}_staging;")

                    logger.info("Truncating staging tables after merge...")
                    for table in tables_in_dependency_order:
                        cur.execute(f"TRUNCATE TABLE {table}_staging;")

                    logger.info("Re-enabling triggers for session...")
                    cur.execute("SET session_replication_role = 'origin';")
                conn.commit()
            logger.info("Merge from staging to production complete.")
        except psycopg2.Error as e:
            logger.error(f"Merge from staging failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def post_load_cleanup(self) -> None:
        logger.info("Performing post-load cleanup (running VACUUM ANALYZE)...")
        # Recreate indexes/FKs would happen here.
        # Running ANALYZE is crucial for the query planner after a large load.
        try:
            with self._get_conn() as conn:
                # Autocommit mode for VACUUM
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("VACUUM ANALYZE;")
                conn.autocommit = False
            logger.info("Post-load cleanup complete.")
        except psycopg2.Error as e:
            logger.error(f"Post-load cleanup failed: {e}")
            raise

    def start_run(self, mode: str) -> int:
        """Creates a new entry in etl_load_history and returns the run_id."""
        logger.info(f"Starting new ETL run. Mode: {mode}")
        sql = """
            INSERT INTO etl_load_history (start_time, status, mode)
            VALUES (NOW(), 'RUNNING', %s)
            RETURNING run_id;
        """
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (mode,))
                    run_id = cur.fetchone()[0]
                conn.commit()
            logger.info(f"ETL run started with run_id: {run_id}")
            return run_id
        except psycopg2.Error as e:
            logger.error(f"Failed to start ETL run: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def end_run(
        self, run_id: int, status: str, records_loaded: int = 0, error_log: str | None = None
    ) -> None:
        """Updates the etl_load_history record for the completed run."""
        logger.info(f"Ending ETL run {run_id} with status: {status}")
        sql = """
            UPDATE etl_load_history
            SET end_time = NOW(), status = %s, records_loaded = %s, error_log = %s
            WHERE run_id = %s;
        """
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (status, records_loaded, error_log, run_id))
                conn.commit()
            logger.info(f"ETL run {run_id} updated successfully.")
        except psycopg2.Error as e:
            logger.error(f"Failed to end ETL run {run_id}: {e}")
            if self.conn:
                self.conn.rollback()
            raise
