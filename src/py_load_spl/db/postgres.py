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

    def get_processed_archives(self) -> set[str]:
        """Retrieves the set of already processed archive names from the database."""
        logger.info("Fetching list of processed archives from the database...")
        sql = "SELECT archive_name FROM etl_processed_archives;"
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    # Use a set comprehension for efficiency
                    processed_archives = {row[0] for row in cur.fetchall()}
            logger.info(f"Found {len(processed_archives)} previously processed archives.")
            return processed_archives
        except psycopg2.Error as e:
            logger.error(f"Failed to fetch processed archives: {e}")
            # In case of an error, assume no archives have been processed
            # to avoid missing data, though this could lead to reprocessing.
            return set()

    def record_processed_archive(self, archive_name: str, checksum: str) -> None:
        """Inserts or updates a record for a successfully processed archive."""
        logger.info(f"Recording '{archive_name}' as processed.")
        sql = """
            INSERT INTO etl_processed_archives (archive_name, archive_checksum, processed_timestamp)
            VALUES (%s, %s, NOW())
            ON CONFLICT (archive_name) DO UPDATE SET
                archive_checksum = EXCLUDED.archive_checksum,
                processed_timestamp = EXCLUDED.processed_timestamp;
        """
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (archive_name, checksum))
                conn.commit()
        except psycopg2.Error as e:
            logger.error(f"Failed to record processed archive {archive_name}: {e}")
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
                        # Define the columns for tables where we are not providing the surrogate key
                        # The order must match the Pydantic model field order.
                        table_columns = {
                            "ingredients_staging": "(document_id, ingredient_name, substance_code, strength_numerator, strength_denominator, unit_of_measure, is_active_ingredient)",
                            "packaging_staging": "(document_id, package_ndc, package_description, package_type)",
                            "marketing_status_staging": "(document_id, marketing_category, start_date, end_date)",
                            "product_ndcs_staging": "(document_id, ndc_code)",
                        }
                        column_spec = table_columns.get(table_name, "")

                        # F007.1: Use copy_expert for efficient bulk loading
                        sql = f"""
                            COPY {table_name} {column_spec} FROM STDIN
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
        For 'delta-load', it performs an UPSERT and replaces child records.
        """
        logger.info(f"Merging data from staging to production (mode: {mode})...")

        # The order is important to respect foreign key constraints.
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
                    logger.info("Disabling triggers for session for performance...")
                    cur.execute("SET session_replication_role = 'replica';")

                    if mode == "full-load":
                        logger.info("Truncating production tables for full load...")
                        for table in reversed(tables_in_dependency_order):
                            logger.debug(f"Truncating {table}...")
                            cur.execute(f"TRUNCATE TABLE {table} CASCADE;")

                        logger.info("Inserting all data from staging...")
                        for table in tables_in_dependency_order:
                            logger.debug(f"Loading data into {table}...")
                            cur.execute(f"INSERT INTO {table} SELECT * FROM {table}_staging;")

                    elif mode == "delta-load":
                        logger.info("Performing delta merge (UPSERT)...")
                        # 1. UPSERT spl_raw_documents
                        update_cols_raw = ["set_id", "version_number", "effective_time", "raw_data", "source_filename", "loaded_at"]
                        update_clause_raw = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols_raw])
                        cur.execute(f"""
                            INSERT INTO spl_raw_documents SELECT * FROM spl_raw_documents_staging
                            ON CONFLICT (document_id) DO UPDATE SET {update_clause_raw};
                        """)

                        # 2. UPSERT products
                        update_cols_prod = ["set_id", "version_number", "effective_time", "product_name", "manufacturer_name", "dosage_form", "route_of_administration", "is_latest_version", "loaded_at"]
                        update_clause_prod = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols_prod])
                        cur.execute(f"""
                            INSERT INTO products SELECT * FROM products_staging
                            ON CONFLICT (document_id) DO UPDATE SET {update_clause_prod};
                        """)

                        # 3. DELETE-INSERT for child tables
                        cur.execute("SELECT DISTINCT document_id FROM products_staging;")
                        doc_ids_to_update = tuple([row[0] for row in cur.fetchall()])

                        if doc_ids_to_update:
                            child_tables = ["product_ndcs", "ingredients", "packaging", "marketing_status"]
                            for table in child_tables:
                                logger.debug(f"Replacing child records in {table} for updated documents...")
                                cur.execute(f"DELETE FROM {table} WHERE document_id IN %s;", (doc_ids_to_update,))
                                cur.execute(f"INSERT INTO {table} SELECT * FROM {table}_staging;")

                    # Final step: Update the is_latest_version flag for all affected products.
                    # This is done for both full and delta loads to ensure consistency.
                    # NOTE: This query relies on the transaction's visibility of the rows
                    # inserted/updated in the 'products' table earlier in this same transaction.
                    # It correctly identifies all set_ids from the staging batch and then re-ranks
                    # all versions (including new ones) for those set_ids in the main products table.
                    logger.info("Updating is_latest_version flag for all affected products...")
                    cur.execute("""
                        WITH affected_set_ids AS (
                            SELECT DISTINCT set_id FROM products_staging
                        ),
                        ranked_products AS (
                            SELECT
                                document_id,
                                ROW_NUMBER() OVER(
                                    PARTITION BY set_id ORDER BY version_number DESC, effective_time DESC
                                ) as rn
                            FROM products
                            WHERE set_id IN (SELECT set_id FROM affected_set_ids)
                        )
                        UPDATE products
                        SET is_latest_version = (ranked_products.rn = 1)
                        FROM ranked_products
                        WHERE products.document_id = ranked_products.document_id;
                    """)

                    # Truncate all staging tables after the merge is complete
                    logger.info("Truncating staging tables...")
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
