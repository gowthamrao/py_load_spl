import io
import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

import psycopg2
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
from psycopg2.extensions import connection

from ..config import DatabaseSettings, PostgresSettings
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
        assert isinstance(db_settings, PostgresSettings), (
            "PostgresLoader requires a PostgresSettings object"
        )
        self.settings = db_settings
        self.conn: connection | None = None
        # Store definitions of dropped objects (indexes, FKs) to recreate them later
        self.dropped_object_definitions: list[str] = []
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
            logger.info(
                f"Found {len(processed_archives)} previously processed archives."
            )
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

    def bulk_load_to_staging(self, intermediate_dir: Path) -> int:
        """
        Loads data from intermediate files (CSV or Parquet) into staging tables.
        Parquet files are converted to a CSV representation in-memory before loading.

        :return: The total number of rows loaded.
        """
        logger.info(f"Bulk loading data from {intermediate_dir} into staging tables...")
        total_rows_loaded = 0
        files_to_process = list(intermediate_dir.glob("*.csv")) + list(
            intermediate_dir.glob("*.parquet")
        )

        if not files_to_process:
            logger.warning(f"No intermediate files found in {intermediate_dir}.")
            return 0

        # Define the columns for tables where we are not providing the surrogate key.
        # The order must match the Pydantic model field order.
        table_columns = {
            "ingredients_staging": (
                "(document_id, ingredient_name, substance_code, strength_numerator, "
                "strength_denominator, unit_of_measure, is_active_ingredient)"
            ),
            "packaging_staging": (
                "(document_id, package_ndc, package_description, package_type)"
            ),
            "marketing_status_staging": (
                "(document_id, marketing_category, start_date, end_date)"
            ),
            "product_ndcs_staging": "(document_id, ndc_code)",
        }

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    for filepath in files_to_process:
                        table_name = f"{filepath.stem}_staging"
                        column_spec = table_columns.get(table_name, "")
                        logger.info(f"Loading {filepath.name} into {table_name}...")

                        if filepath.suffix == ".csv":
                            sql = f"""
                                COPY {table_name} {column_spec} FROM STDIN
                                WITH (FORMAT CSV, NULL '\\N', QUOTE '\"');
                            """
                            with open(filepath, encoding="utf-8") as f:
                                cur.copy_expert(sql, f)
                                total_rows_loaded += cur.rowcount
                                logger.info(f"Loaded {cur.rowcount} rows.")

                        elif filepath.suffix == ".parquet":
                            sql = f"""
                                COPY {table_name} {column_spec} FROM STDIN
                                WITH (FORMAT CSV, NULL '\\N', QUOTE '\"', HEADER TRUE);
                            """
                            table = pq.read_table(filepath)
                            buffer = io.StringIO()
                            pa_csv.write_csv(table, buffer)
                            buffer.seek(0)
                            cur.copy_expert(sql, buffer)
                            total_rows_loaded += cur.rowcount
                            logger.info(f"Loaded {cur.rowcount} rows.")

                conn.commit()
            logger.info(
                "Bulk load to staging tables complete. Total rows loaded: %d",
                total_rows_loaded,
            )
            return total_rows_loaded
        except (OSError, psycopg2.Error, pa.ArrowException) as e:
            logger.error(f"Bulk load to staging failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def _get_optimizable_objects(self, cur: Any) -> None:
        """
        Dynamically queries the database to get the definitions of all foreign keys
        and indexes on the production tables. This is more robust than hardcoding.
        """
        logger.info("Querying database for existing indexes and foreign keys...")
        # Query for foreign key constraints
        cur.execute("""
            SELECT 'ALTER TABLE ' || n.nspname || '."' || conrel.relname || '" ADD CONSTRAINT "' || c.conname || '" ' || pg_get_constraintdef(c.oid) || ';'
            FROM pg_constraint c
            JOIN pg_class conrel ON conrel.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = conrel.relnamespace
            WHERE c.contype = 'f' AND conrel.relname IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status'
            );
        """)
        fk_defs = [row[0] for row in cur.fetchall()]
        logger.info(f"Found {len(fk_defs)} foreign key constraints.")

        # Query for index definitions
        cur.execute("""
            SELECT indexdef
            FROM pg_indexes
            WHERE tablename IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status', 'spl_raw_documents'
            ) AND indexname NOT LIKE 'pg_%%' AND indexname NOT IN (
                -- Exclude primary key indexes, which are not dropped
                'products_pkey', 'product_ndcs_pkey', 'ingredients_pkey', 'packaging_pkey', 'marketing_status_pkey', 'spl_raw_documents_pkey'
            );
        """)
        idx_defs = [row[0] for row in cur.fetchall()]
        logger.info(f"Found {len(idx_defs)} indexes.")

        self.dropped_object_definitions = fk_defs + idx_defs

    def _drop_optimizations(self, cur: Any) -> None:
        """Drops all foreign keys and indexes found by the getter."""
        if not self.dropped_object_definitions:
            logger.warning("No optimizable objects found to drop.")
            return

        logger.info(
            f"Dropping {len(self.dropped_object_definitions)} indexes and foreign keys..."
        )
        # Drop foreign keys first
        cur.execute("""
            SELECT 'ALTER TABLE ' || n.nspname || '."' || conrel.relname || '" DROP CONSTRAINT "' || c.conname || '";'
            FROM pg_constraint c
            JOIN pg_class conrel ON conrel.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = conrel.relnamespace
            WHERE c.contype = 'f' AND conrel.relname IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status'
            );
        """)
        for row in cur.fetchall():
            logger.debug(f"Executing: {row[0]}")
            cur.execute(row[0])

        # Then drop indexes
        cur.execute("""
            SELECT 'DROP INDEX IF EXISTS "' || indexname || '";'
            FROM pg_indexes
            WHERE tablename IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status', 'spl_raw_documents'
            ) AND indexname NOT LIKE 'pg_%%' AND indexname NOT IN (
                 'products_pkey', 'product_ndcs_pkey', 'ingredients_pkey', 'packaging_pkey', 'marketing_status_pkey', 'spl_raw_documents_pkey'
            );
        """)
        for row in cur.fetchall():
            logger.debug(f"Executing: {row[0]}")
            cur.execute(row[0])
        logger.info("Finished dropping objects.")

    def _recreate_optimizations(self, cur: Any) -> None:
        """Recreates all the objects that were previously dropped."""
        if not self.dropped_object_definitions:
            logger.warning("No stored object definitions to recreate.")
            return

        logger.info(
            f"Recreating {len(self.dropped_object_definitions)} indexes and foreign keys..."
        )
        for definition in self.dropped_object_definitions:
            logger.debug(f"Executing: {definition}")
            cur.execute(definition)
        logger.info("Finished recreating objects.")
        # Clear the list after we're done
        self.dropped_object_definitions = []

    def pre_load_optimization(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        For full loads, drops indexes and foreign keys to speed up data insertion.
        For delta loads, this step is skipped.
        """
        logger.info("Performing pre-load optimizations...")
        if mode != "full-load" or not self.settings.optimize_full_load:
            logger.info("Skipping index/FK drop for this run.")
            return

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    self._get_optimizable_objects(cur)
                    self._drop_optimizations(cur)
                conn.commit()
            logger.info("Pre-load optimization step complete.")
        except psycopg2.Error as e:
            logger.error(f"Pre-load optimization failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

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
                        # We don't need CASCADE anymore since we dropped the FKs
                        for table in reversed(tables_in_dependency_order):
                            logger.debug(f"Truncating {table}...")
                            cur.execute(f"TRUNCATE TABLE {table};")

                        logger.info("Inserting all data from staging...")
                        for table in tables_in_dependency_order:
                            logger.debug(f"Loading data into {table}...")
                            cur.execute(
                                f"INSERT INTO {table} SELECT * FROM {table}_staging;"
                            )

                    elif mode == "delta-load":
                        logger.info("Performing delta merge (UPSERT)...")
                        # 1. UPSERT spl_raw_documents
                        update_cols_raw = [
                            "set_id",
                            "version_number",
                            "effective_time",
                            "raw_data",
                            "source_filename",
                            "loaded_at",
                        ]
                        update_clause_raw = ", ".join(
                            [f"{col} = EXCLUDED.{col}" for col in update_cols_raw]
                        )
                        cur.execute(f"""
                            INSERT INTO spl_raw_documents SELECT * FROM spl_raw_documents_staging
                            ON CONFLICT (document_id) DO UPDATE SET {update_clause_raw};
                        """)

                        # 2. UPSERT products
                        update_cols_prod = [
                            "set_id",
                            "version_number",
                            "effective_time",
                            "product_name",
                            "manufacturer_name",
                            "dosage_form",
                            "route_of_administration",
                            "is_latest_version",
                            "loaded_at",
                        ]
                        update_clause_prod = ", ".join(
                            [f"{col} = EXCLUDED.{col}" for col in update_cols_prod]
                        )
                        cur.execute(f"""
                            INSERT INTO products SELECT * FROM products_staging
                            ON CONFLICT (document_id) DO UPDATE SET {update_clause_prod};
                        """)

                        # 3. DELETE-INSERT for child tables
                        cur.execute(
                            "SELECT DISTINCT document_id FROM products_staging;"
                        )
                        doc_ids_to_update = tuple([row[0] for row in cur.fetchall()])

                        if doc_ids_to_update:
                            child_tables = [
                                "product_ndcs",
                                "ingredients",
                                "packaging",
                                "marketing_status",
                            ]
                            for table in child_tables:
                                logger.debug(
                                    f"Replacing child records in {table} for updated documents..."
                                )
                                cur.execute(
                                    f"DELETE FROM {table} WHERE document_id IN %s;",
                                    (doc_ids_to_update,),
                                )
                                cur.execute(
                                    f"INSERT INTO {table} SELECT * FROM {table}_staging;"
                                )

                    # Final step: Update the is_latest_version flag for all affected products.
                    # This is done for both full and delta loads to ensure consistency.
                    logger.info(
                        "Updating is_latest_version flag for all affected products..."
                    )
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

    def post_load_cleanup(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        For full loads, recreates the indexes and FKs.
        For all loads, runs VACUUM ANALYZE for query planner optimization.
        """
        logger.info("Performing post-load cleanup...")
        try:
            # Step 1: Recreate optimizations, if applicable. This is done in its own
            # transaction which is committed immediately after.
            if mode == "full-load" and self.settings.optimize_full_load:
                with self._get_conn() as conn:
                    with conn.cursor() as cur:
                        self._recreate_optimizations(cur)
                    conn.commit()

            # Step 2: Run VACUUM ANALYZE. This must be done outside a transaction
            # block, so we use a connection in autocommit mode.
            with self._get_conn() as conn:
                logger.info("Running VACUUM ANALYZE...")
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("VACUUM (VERBOSE, ANALYZE);")
                conn.autocommit = False

            logger.info("Post-load cleanup complete.")
        except psycopg2.Error as e:
            logger.error(f"Post-load cleanup failed: {e}", exc_info=True)
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
                    result = cur.fetchone()
                    if result is None:
                        raise RuntimeError("Failed to get run_id from database.")
                    run_id: int = result[0]
                conn.commit()
            logger.info(f"ETL run started with run_id: {run_id}")
            return run_id
        except psycopg2.Error as e:
            logger.error(f"Failed to start ETL run: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def end_run(
        self,
        run_id: int,
        status: str,
        records_loaded: int = 0,
        error_log: str | None = None,
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
