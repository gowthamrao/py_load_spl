import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import redshift_connector
from redshift_connector.core import Connection

from ..config import RedshiftSettings, S3Settings
from ..s3 import S3Uploader
from .base import DatabaseLoader

logger = logging.getLogger(__name__)

SQL_SCHEMA_PATH = Path(__file__).parent / "sql/redshift_schema.sql"


class RedshiftLoader(DatabaseLoader):
    """
    Amazon Redshift-specific implementation of the DatabaseLoader.

    This adapter works by first uploading intermediate files to an S3 bucket
    and then using the Redshift `COPY` command to load the data.
    """

    def __init__(self, db_settings: RedshiftSettings, s3_settings: S3Settings) -> None:
        self.settings = db_settings
        self.s3_uploader = S3Uploader(s3_settings)
        self.conn: Connection | None = None
        self.dropped_object_definitions: list[str] = []
        logger.info("Initialized Redshift Loader.")

    @contextmanager
    def _get_conn(self) -> Generator[Connection, None, None]:
        """Establish and manage a database connection."""
        if self.conn is None or self.conn.closed:
            try:
                logger.info(
                    f"Connecting to Redshift at {self.settings.host}:{self.settings.port}..."
                )
                self.conn = redshift_connector.connect(
                    database=self.settings.name,
                    user=self.settings.user,
                    password=self.settings.password,
                    host=self.settings.host,
                    port=self.settings.port,
                )
            except redshift_connector.Error as e:
                logger.error(f"Database connection failed: {e}")
                raise
        yield self.conn

    def initialize_schema(self) -> None:
        """Creates the necessary tables and structures from the DDL file."""
        logger.info("Initializing Redshift schema...")
        if not SQL_SCHEMA_PATH.exists():
            raise FileNotFoundError(f"Schema file not found at {SQL_SCHEMA_PATH}")

        ddl = SQL_SCHEMA_PATH.read_text()

        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    # Redshift may not support executing the whole file at once if it contains multiple statements separated by ';'.
                    # It's safer to split and execute one by one.
                    for statement in ddl.split(";\n"):
                        if statement.strip():
                            cur.execute(statement)
                conn.commit()
            logger.info("Schema initialization complete.")
        except redshift_connector.Error as e:
            logger.error(f"Schema initialization failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def bulk_load_to_staging(self, intermediate_dir: Path) -> int:
        """
        Uploads files to S3 and then uses COPY to load into Redshift.

        :return: The total number of rows loaded.
        """
        logger.info(
            f"Starting bulk load process for Redshift from {intermediate_dir}..."
        )
        total_rows_loaded = 0

        # Step 1: Upload all intermediate files to S3
        s3_uri = self.s3_uploader.upload_directory(intermediate_dir)
        logger.info(f"Intermediate files uploaded to {s3_uri}.")

        files_to_process = list(intermediate_dir.glob("*.csv")) + list(
            intermediate_dir.glob("*.parquet")
        )

        if not files_to_process:
            logger.warning(f"No intermediate files found in {intermediate_dir}.")
            return 0

        # Step 2: Execute COPY command for each file from S3
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    for filepath in files_to_process:
                        table_name = f"{filepath.stem}_staging"
                        s3_key = f"{s3_uri}/{filepath.name}"

                        if filepath.suffix == ".csv":
                            format_options = "FORMAT AS CSV NULL '\\N' QUOTE '\"'"
                        elif filepath.suffix == ".parquet":
                            format_options = "FORMAT AS PARQUET"
                        else:
                            continue

                        logger.info(f"Loading {s3_key} into {table_name}...")
                        sql = f"""
                            COPY {table_name}
                            FROM '{s3_key}'
                            IAM_ROLE '{self.settings.iam_role_arn}'
                            {format_options};
                        """
                        cur.execute(sql)
                        total_rows_loaded += cur.rowcount
                        logger.info(f"Loaded {cur.rowcount} rows.")
                conn.commit()
            logger.info(
                "Bulk load from S3 complete. Total rows loaded: %d", total_rows_loaded
            )
            return total_rows_loaded
        except redshift_connector.Error as e:
            logger.error(f"Bulk load to staging failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def pre_load_optimization(self, mode: Literal["full-load", "delta-load"]) -> None:
        # For now, we are not implementing the more complex index/FK dropping,
        # as it requires querying Redshift-specific system tables. This can be
        # added later as an enhancement.
        logger.info("Skipping pre-load optimizations for Redshift in this version.")
        pass

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        logger.info(f"Merging data from staging to production (mode: {mode})...")
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
                    if mode == "full-load":
                        logger.info("Truncating production tables for full load...")
                        for table in reversed(tables_in_dependency_order):
                            cur.execute(f"TRUNCATE TABLE {table};")
                        logger.info("Inserting all data from staging...")
                        for table in tables_in_dependency_order:
                            cur.execute(
                                f"INSERT INTO {table} SELECT * FROM {table}_staging;"
                            )
                    elif mode == "delta-load":
                        logger.info("Performing delta merge (DELETE/INSERT)...")
                        # For each table, delete the rows that are about to be updated/inserted, then insert the new ones.
                        # This is the standard UPSERT pattern for Redshift.
                        for table in tables_in_dependency_order:
                            logger.debug(f"Merging data for {table}...")
                            # For parent tables, we delete all records that are being updated.
                            # For child tables, we delete all children for the parent documents being updated.
                            if table == "products" or table == "spl_raw_documents":
                                cur.execute(f"""
                                    DELETE FROM {table}
                                    WHERE set_id IN (SELECT DISTINCT set_id FROM {table}_staging);
                                """)
                            else:
                                cur.execute(f"""
                                    DELETE FROM {table}
                                    WHERE document_id IN (SELECT DISTINCT document_id FROM {table}_staging);
                                """)

                            cur.execute(
                                f"INSERT INTO {table} SELECT * FROM {table}_staging;"
                            )

                    logger.info("Updating is_latest_version flag...")
                    cur.execute("""
                        UPDATE products
                        SET is_latest_version = (sub.rn = 1)
                        FROM (
                            SELECT
                                document_id,
                                ROW_NUMBER() OVER(
                                    PARTITION BY set_id ORDER BY version_number DESC, effective_time DESC
                                ) as rn
                            FROM products
                            WHERE set_id IN (SELECT DISTINCT set_id FROM products_staging)
                        ) AS sub
                        WHERE products.document_id = sub.document_id;
                    """)

                    logger.info("Truncating staging tables...")
                    for table in tables_in_dependency_order:
                        cur.execute(f"TRUNCATE TABLE {table}_staging;")
                conn.commit()
        except redshift_connector.Error as e:
            logger.error(f"Merge from staging failed: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def post_load_cleanup(self, mode: Literal["full-load", "delta-load"]) -> None:
        logger.info("Performing post-load cleanup (VACUUM and ANALYZE)...")
        try:
            with self._get_conn() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    logger.info("Running VACUUM...")
                    cur.execute("VACUUM;")
                    logger.info("Running ANALYZE...")
                    cur.execute("ANALYZE;")
                conn.autocommit = False
            logger.info("Post-load cleanup complete.")
        except redshift_connector.Error as e:
            logger.error(f"Post-load cleanup failed: {e}", exc_info=True)
            raise

    def start_run(self, mode: str) -> int:
        sql = "INSERT INTO etl_load_history (start_time, status, mode) VALUES (NOW(), 'RUNNING', %s);"
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (mode,))
                    # Redshift does not have a RETURNING clause like Postgres.
                    # We need to get the last generated identity value.
                    cur.execute("SELECT MAX(run_id) FROM etl_load_history;")
                    row = cur.fetchone()
                    if row:
                        run_id: int = row[0]
                        conn.commit()
                        logger.info(f"ETL run started with run_id: {run_id}")
                        return run_id
                raise RuntimeError("Could not retrieve run_id after starting run.")
        except redshift_connector.Error as e:
            logger.error(f"Failed to start ETL run: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:
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
        except redshift_connector.Error as e:
            logger.error(f"Failed to end ETL run {run_id}: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            raise

    def get_processed_archives(self) -> set[str]:
        sql = "SELECT archive_name FROM etl_processed_archives;"
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    processed_archives = {row[0] for row in cur.fetchall()}
            return processed_archives
        except redshift_connector.Error as e:
            logger.error(f"Failed to fetch processed archives: {e}", exc_info=True)
            return set()

    def record_processed_archive(self, archive_name: str, checksum: str) -> None:
        logger.info(f"Recording '{archive_name}' as processed in Redshift.")
        try:
            with self._get_conn() as conn:
                with conn.cursor() as cur:
                    # Redshift UPSERT pattern: DELETE then INSERT
                    cur.execute(
                        "DELETE FROM etl_processed_archives WHERE archive_name = %s;",
                        (archive_name,),
                    )
                    cur.execute(
                        """
                        INSERT INTO etl_processed_archives (archive_name, archive_checksum, processed_timestamp)
                        VALUES (%s, %s, NOW());
                    """,
                        (archive_name, checksum),
                    )
                conn.commit()
        except redshift_connector.Error as e:
            logger.error(
                f"Failed to record processed archive {archive_name}: {e}", exc_info=True
            )
            if self.conn:
                self.conn.rollback()
            raise
