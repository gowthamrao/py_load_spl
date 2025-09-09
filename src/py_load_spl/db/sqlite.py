import csv
import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from ..config import DatabaseSettings
from .base import DatabaseLoader

logger = logging.getLogger(__name__)

SQL_SCHEMA_PATH = Path(__file__).parent / "sql/sqlite_schema.sql"


class SqliteLoader(DatabaseLoader):
    """SQLite-specific implementation of the DatabaseLoader."""

    def __init__(self, db_settings: DatabaseSettings) -> None:
        self.settings = db_settings
        # For SQLite, the 'name' from settings is interpreted as the file path.
        self.db_path = Path(self.settings.name)
        self.conn: sqlite3.Connection | None = None
        logger.info(f"Initialized SQLite Loader for database at: {self.db_path}")

    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Establish and manage a database connection."""
        if self.conn is None:
            try:
                logger.info(f"Connecting to SQLite database: {self.db_path}...")
                # Ensure the parent directory exists
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.db_path)
                # Improve performance and allow concurrent writes
                self.conn.execute("PRAGMA journal_mode = WAL;")
            except sqlite3.Error as e:
                logger.error(f"SQLite connection failed: {e}")
                raise
        yield self.conn

    def initialize_schema(self) -> None:
        """Creates the necessary tables and structures from the DDL file."""
        logger.info(f"Initializing SQLite schema from {SQL_SCHEMA_PATH}...")
        if not SQL_SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found at {SQL_SCHEMA_PATH}")

        ddl = SQL_SCHEMA_PATH.read_text()

        try:
            with self._get_conn() as conn:
                conn.executescript(ddl)
                conn.commit()
            logger.info("SQLite schema initialization complete.")
        except sqlite3.Error as e:
            logger.error(f"SQLite schema initialization failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def bulk_load_to_staging(self, intermediate_dir: Path) -> None:
        """
        Loads data from intermediate files into staging tables.
        Since SQLite doesn't have a COPY command, we read the files and use
        `executemany` for bulk insertion.
        """
        logger.info(
            f"Bulk loading data from {intermediate_dir} into SQLite staging tables..."
        )
        files_to_process = list(intermediate_dir.glob("*.csv"))
        # Note: Parquet loading is omitted for this initial implementation for simplicity.
        # A full implementation would read parquet into a list of tuples/dicts.

        if not files_to_process:
            logger.warning(f"No intermediate CSV files found in {intermediate_dir}.")
            return

        # This mapping is based on the order of fields in the Pydantic models.
        # It's crucial that the schema of the staging tables matches this order.
        table_column_counts = {
            "products_staging": 10,
            "spl_raw_documents_staging": 7,
            "product_ndcs_staging": 2,
            "ingredients_staging": 7,
            "packaging_staging": 4,
            "marketing_status_staging": 4,
        }

        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                for filepath in files_to_process:
                    table_name = f"{filepath.stem}_staging"
                    if table_name not in table_column_counts:
                        logger.warning(
                            f"No staging table mapping for file {filepath.name}, skipping."
                        )
                        continue

                    col_count = table_column_counts[table_name]
                    placeholders = ", ".join(["?"] * col_count)
                    sql = f"INSERT INTO {table_name} VALUES ({placeholders});"
                    logger.info(f"Loading {filepath.name} into {table_name}...")

                    with open(filepath, "r", encoding="utf-8") as f:
                        reader = csv.reader(f)
                        # Convert '\\N' back to None for SQLite
                        data_to_load = [
                            tuple(None if cell == "\\N" else cell for cell in row)
                            for row in reader
                        ]

                    if data_to_load:
                        cur.executemany(sql, data_to_load)
                        logger.info(
                            f"Loaded {len(data_to_load)} records into {table_name}."
                        )
                conn.commit()
            logger.info("Bulk load to staging tables complete.")
        except (OSError, sqlite3.Error) as e:
            logger.error(f"SQLite bulk load to staging failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def pre_load_optimization(self, mode: Literal["full-load", "delta-load"]) -> None:
        """Optional: No-op for SQLite in this implementation."""
        logger.info("Skipping pre-load optimizations for SQLite.")
        pass

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        Merges data from staging to production tables using a single transaction.
        This version contains fixes for SQLite syntax and logic.
        """
        logger.info(f"Merging data from staging to production (mode: {mode})...")
        parent_tables = ["spl_raw_documents", "products"]
        child_tables = ["product_ndcs", "ingredients", "packaging", "marketing_status"]
        all_tables = parent_tables + child_tables

        child_columns = {
            "product_ndcs": "(document_id, ndc_code)",
            "ingredients": "(document_id, ingredient_name, substance_code, strength_numerator, strength_denominator, unit_of_measure, is_active_ingredient)",
            "packaging": "(document_id, package_ndc, package_description, package_type)",
            "marketing_status": "(document_id, marketing_category, start_date, end_date)",
        }

        try:
            with self._get_conn() as conn:
                cur = conn.cursor()

                if mode == "full-load":
                    logger.info("Truncating production tables for full load...")
                    for table in reversed(all_tables):
                        cur.execute(f"DELETE FROM {table};")

                    logger.info("Inserting all data from staging...")
                    for table in parent_tables:
                        cur.execute(f"INSERT INTO {table} SELECT * FROM {table}_staging;")
                    for table in child_tables:
                        cols = child_columns[table]
                        cur.execute(f"INSERT INTO {table} {cols} SELECT * FROM {table}_staging;")

                elif mode == "delta-load":
                    logger.info("Performing delta merge (using REPLACE)...")
                    # 1. Use REPLACE for parent tables. This is equivalent to DELETE then INSERT.
                    cur.execute(
                        "REPLACE INTO spl_raw_documents SELECT * FROM spl_raw_documents_staging;"
                    )
                    cur.execute("REPLACE INTO products SELECT * FROM products_staging;")

                    # 2. DELETE-INSERT for Child Tables
                    cur.execute("SELECT DISTINCT document_id FROM products_staging;")
                    doc_ids_to_update = tuple(row[0] for row in cur.fetchall())

                    if doc_ids_to_update:
                        for table in child_tables:
                            # Use `IN` with a tuple for the DELETE
                            cur.execute(
                                f"DELETE FROM {table} WHERE document_id IN ({','.join('?' for _ in doc_ids_to_update)})",
                                doc_ids_to_update,
                            )
                            # Insert all records from staging for that table
                            cols = child_columns[table]
                            cur.execute(f"INSERT INTO {table} {cols} SELECT * FROM {table}_staging;")

                # Update is_latest_version flag using a SQLite-compatible window function approach
                logger.info("Updating is_latest_version flag for affected products...")
                cur.execute("""
                    UPDATE products SET is_latest_version = 0
                    WHERE set_id IN (SELECT DISTINCT set_id FROM products_staging);
                """)
                cur.execute("""
                    UPDATE products SET is_latest_version = 1
                    WHERE document_id IN (
                        SELECT document_id FROM (
                            SELECT
                                document_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY set_id ORDER BY version_number DESC, effective_time DESC
                                ) as rn
                            FROM products
                            WHERE set_id IN (SELECT DISTINCT set_id FROM products_staging)
                        ) WHERE rn = 1
                    );
                """)

                # Truncate all staging tables
                logger.info("Truncating staging tables...")
                for table in all_tables:
                    cur.execute(f"DELETE FROM {table}_staging;")

                conn.commit()
            logger.info("Merge from staging to production complete.")
        except sqlite3.Error as e:
            logger.error(f"SQLite merge from staging failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def post_load_cleanup(self, mode: Literal["full-load", "delta-load"]) -> None:
        """Runs VACUUM to optimize the database file."""
        logger.info("Performing post-load cleanup (VACUUM)...")
        try:
            with self._get_conn() as conn:
                conn.execute("VACUUM;")
                conn.commit()
            logger.info("Post-load cleanup complete.")
        except sqlite3.Error as e:
            logger.error(f"SQLite post-load cleanup failed: {e}", exc_info=True)
            raise

    def start_run(self, mode: Literal["full-load", "delta-load"]) -> int:
        """Creates a new entry in etl_load_history and returns the run_id."""
        logger.info(f"Starting new ETL run. Mode: {mode}")
        sql = "INSERT INTO etl_load_history (start_time, status, mode) VALUES (datetime('now'), 'RUNNING', ?);"
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql, (mode,))
                run_id = cur.lastrowid
                conn.commit()
            logger.info(f"ETL run started with run_id: {run_id}")
            return run_id
        except sqlite3.Error as e:
            logger.error(f"Failed to start ETL run: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:
        """Updates the etl_load_history record for the completed run."""
        logger.info(f"Ending ETL run {run_id} with status: {status}")
        sql = "UPDATE etl_load_history SET end_time = datetime('now'), status = ?, records_loaded = ?, error_log = ? WHERE run_id = ?;"
        try:
            with self._get_conn() as conn:
                conn.execute(sql, (status, records_loaded, error_log, run_id))
                conn.commit()
            logger.info(f"ETL run {run_id} updated successfully.")
        except sqlite3.Error as e:
            logger.error(f"Failed to end ETL run {run_id}: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def get_processed_archives(self) -> set[str]:
        """Retrieves the set of already processed archive names from the database."""
        logger.info("Fetching list of processed archives from the database...")
        sql = "SELECT archive_name FROM etl_processed_archives;"
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                processed_archives = {row[0] for row in cur.fetchall()}
            logger.info(
                f"Found {len(processed_archives)} previously processed archives."
            )
            return processed_archives
        except sqlite3.Error as e:
            logger.error(f"Failed to fetch processed archives: {e}", exc_info=True)
            return set()

    def record_processed_archive(self, archive_name: str, checksum: str) -> None:
        """Inserts or updates a record for a successfully processed archive."""
        logger.info(f"Recording '{archive_name}' as processed.")
        sql = """
            INSERT INTO etl_processed_archives (archive_name, archive_checksum, processed_timestamp)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT (archive_name) DO UPDATE SET
                archive_checksum = excluded.archive_checksum,
                processed_timestamp = excluded.processed_timestamp;
        """
        try:
            with self._get_conn() as conn:
                conn.execute(sql, (archive_name, checksum))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(
                f"Failed to record processed archive {archive_name}: {e}", exc_info=True
            )
            if self.conn:
                self.conn.rollback()
            raise
