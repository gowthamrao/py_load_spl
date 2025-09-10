import csv
import logging
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

from ..config import DatabaseSettings, SqliteSettings
from .base import DatabaseLoader

logger = logging.getLogger(__name__)

SQL_SCHEMA_PATH = Path(__file__).parent / "sql/sqlite_schema.sql"

TABLE_COLUMNS_MAP = {
    "products": (
        "document_id, set_id, version_number, effective_time, product_name, "
        "manufacturer_name, dosage_form, route_of_administration, "
        "is_latest_version, loaded_at"
    ),
    "spl_raw_documents": (
        "document_id, set_id, version_number, effective_time, raw_data, "
        "source_filename, loaded_at"
    ),
    "product_ndcs": "document_id, ndc_code",
    "ingredients": (
        "document_id, ingredient_name, substance_code, strength_numerator, "
        "strength_denominator, unit_of_measure, is_active_ingredient"
    ),
    "packaging": "document_id, package_ndc, package_description, package_type",
    "marketing_status": "document_id, marketing_category, start_date, end_date",
}


class SqliteLoader(DatabaseLoader):
    """SQLite-specific implementation of the DatabaseLoader."""

    def __init__(self, db_settings: DatabaseSettings) -> None:
        assert isinstance(
            db_settings, SqliteSettings
        ), "SqliteLoader requires a SqliteSettings object"
        self.settings = db_settings
        self.db_path = Path(self.settings.name)
        self.conn: sqlite3.Connection | None = None
        logger.info(f"Initialized SQLite Loader for database at: {self.db_path}")

    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Establishes and manages a database connection.
        This version maintains a persistent connection for the lifetime of the loader.
        """
        if self.conn is None:
            try:
                logger.info(f"Connecting to SQLite database: {self.db_path}...")
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                self.conn = sqlite3.connect(self.db_path)
                self.conn.execute("PRAGMA journal_mode = WAL;")
                self.conn.execute("PRAGMA foreign_keys = ON;")
            except sqlite3.Error as e:
                logger.error(f"SQLite connection failed: {e}", exc_info=True)
                raise
        yield self.conn

    def close_conn(self) -> None:
        """Explicitly closes the connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def initialize_schema(self) -> None:
        logger.info(f"Initializing SQLite schema from {SQL_SCHEMA_PATH}...")
        if not SQL_SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found at {SQL_SCHEMA_PATH}")
        ddl = SQL_SCHEMA_PATH.read_text()
        try:
            with self._get_conn() as conn:
                conn.executescript(ddl)
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite schema initialization failed: {e}", exc_info=True)
            raise

    def bulk_load_to_staging(self, intermediate_dir: Path) -> None:
        logger.info(
            f"Bulk loading data from {intermediate_dir} into SQLite staging tables..."
        )
        files_to_process = list(intermediate_dir.glob("*.csv"))
        if not files_to_process:
            logger.warning(f"No intermediate CSV files found in {intermediate_dir}.")
            return
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                for filepath in files_to_process:
                    table_base_name = filepath.stem
                    table_name = f"{table_base_name}_staging"
                    column_spec = TABLE_COLUMNS_MAP.get(table_base_name)
                    if not column_spec:
                        logger.warning(f"No column mapping for {table_name}, skipping.")
                        continue
                    num_columns = column_spec.count(",") + 1
                    placeholders = ", ".join(["?"] * num_columns)
                    sql = f"INSERT INTO {table_name} ({column_spec}) VALUES ({placeholders});"
                    batch_size = 20000
                    batch = []
                    with open(filepath, encoding="utf-8") as f:
                        reader = csv.reader(f)
                        for row in reader:
                            processed_row = tuple(
                                None if cell == "\\N" else cell for cell in row
                            )
                            batch.append(processed_row)
                            if len(batch) >= batch_size:
                                cur.executemany(sql, batch)
                                batch.clear()
                        if batch:
                            cur.executemany(sql, batch)
                conn.commit()
        except (OSError, sqlite3.Error) as e:
            logger.error(f"SQLite bulk load to staging failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def pre_load_optimization(self, mode: Literal["full-load", "delta-load"]) -> None:
        if mode == "full-load" and self.settings.optimize_full_load:
            logger.info("Disabling foreign key checks for full load.")
            try:
                with self._get_conn() as conn:
                    conn.execute("PRAGMA foreign_keys = OFF;")
            except sqlite3.Error as e:
                logger.error(f"Failed to disable foreign keys: {e}", exc_info=True)
                raise
        else:
            logger.info("Skipping pre-load optimizations.")

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        logger.info(f"Merging data from staging to production (mode: {mode})...")
        parent_tables = ["spl_raw_documents", "products"]
        child_tables = ["product_ndcs", "ingredients", "packaging", "marketing_status"]
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                if mode == "full-load":
                    for table in reversed(parent_tables + child_tables):
                        cur.execute(f"DELETE FROM {table};")
                    for table in parent_tables:
                        # For tables with matching columns, we can still use SELECT *
                        cur.execute(
                            f"INSERT INTO {table} SELECT * FROM {table}_staging;"
                        )
                    for table in child_tables:
                        cols = TABLE_COLUMNS_MAP[table]
                        cur.execute(
                            f"INSERT INTO {table} ({cols}) SELECT * FROM {table}_staging;"
                        )
                elif mode == "delta-load":
                    cur.execute(
                        "REPLACE INTO spl_raw_documents SELECT * FROM spl_raw_documents_staging;"
                    )
                    cur.execute("REPLACE INTO products SELECT * FROM products_staging;")
                    cur.execute("SELECT DISTINCT document_id FROM products_staging;")
                    doc_ids_to_update = tuple(row[0] for row in cur.fetchall())
                    if doc_ids_to_update:
                        for table in child_tables:
                            q_marks = ",".join("?" * len(doc_ids_to_update))
                            cur.execute(
                                f"DELETE FROM {table} WHERE document_id IN ({q_marks})",
                                doc_ids_to_update,
                            )
                            cols = TABLE_COLUMNS_MAP[table]
                            cur.execute(
                                f"INSERT INTO {table} ({cols}) SELECT * FROM {table}_staging;"
                            )
                self.update_latest_version_flag(cur)
                for table in parent_tables + child_tables:
                    cur.execute(f"DELETE FROM {table}_staging;")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite merge failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def update_latest_version_flag(self, cur: sqlite3.Cursor) -> None:
        """Updates the is_latest_version flag for all affected products."""
        logger.info("Updating is_latest_version flag...")
        cur.execute(
            "UPDATE products SET is_latest_version = 0 "
            "WHERE set_id IN (SELECT DISTINCT set_id FROM products_staging);"
        )
        cur.execute(
            """
            UPDATE products SET is_latest_version = 1
            WHERE document_id IN (
                SELECT document_id FROM (
                    SELECT
                        document_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY set_id
                            ORDER BY version_number DESC, effective_time DESC
                        ) as rn
                    FROM products
                    WHERE set_id IN (SELECT DISTINCT set_id FROM products_staging)
                ) WHERE rn = 1
            );
            """
        )

    def post_load_cleanup(self, mode: Literal["full-load", "delta-load"]) -> None:
        logger.info("Performing post-load cleanup...")
        try:
            with self._get_conn() as conn:
                if mode == "full-load" and self.settings.optimize_full_load:
                    logger.info("Re-enabling foreign key checks.")
                    conn.execute("PRAGMA foreign_keys = ON;")
                logger.info("Running VACUUM...")
                conn.execute("VACUUM;")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"SQLite post-load cleanup failed: {e}", exc_info=True)
            raise

    def start_run(self, mode: Literal["full-load", "delta-load"]) -> int:
        sql = "INSERT INTO etl_load_history (start_time, status, mode) VALUES (datetime('now'), 'RUNNING', ?);"
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql, (mode,))
                run_id = cur.lastrowid
                conn.commit()
                return run_id if run_id is not None else -1
        except sqlite3.Error as e:
            logger.error(f"Failed to start ETL run: {e}", exc_info=True)
            raise

    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:
        sql = "UPDATE etl_load_history SET end_time = datetime('now'), status = ?, records_loaded = ?, error_log = ? WHERE run_id = ?;"
        try:
            with self._get_conn() as conn:
                conn.execute(sql, (status, records_loaded, error_log, run_id))
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Failed to end ETL run {run_id}: {e}", exc_info=True)
            raise

    def get_processed_archives(self) -> set[str]:
        sql = "SELECT archive_name FROM etl_processed_archives;"
        try:
            with self._get_conn() as conn:
                cur = conn.cursor()
                cur.execute(sql)
                return {row[0] for row in cur.fetchall()}
        except sqlite3.Error as e:
            logger.error(f"Failed to fetch processed archives: {e}", exc_info=True)
            return set()

    def record_processed_archive(self, archive_name: str, checksum: str) -> None:
        sql = """
            INSERT INTO etl_processed_archives
                (archive_name, archive_checksum, processed_timestamp)
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
            logger.error(f"Failed to record archive {archive_name}: {e}", exc_info=True)
            raise
