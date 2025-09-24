import logging
from pathlib import Path
from typing import Literal

from databricks import sql
from databricks.sql.client import Connection

from ..config import DatabricksSettings, S3Settings
from ..s3 import S3Uploader
from .base import DatabaseLoader

logger = logging.getLogger(__name__)


class DatabricksLoader(DatabaseLoader):
    """DatabaseLoader adapter for Databricks."""

    def __init__(self, settings: DatabricksSettings):
        self.settings = settings
        s3_bucket, s3_prefix = self._parse_s3_path(settings.s3_staging_path)
        s3_settings = S3Settings(bucket=s3_bucket, prefix=s3_prefix)
        self.s3_uploader = S3Uploader(s3_settings)

    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parses an S3 path like 's3://bucket/prefix' into bucket and prefix."""
        if not s3_path.startswith("s3://"):
            raise ValueError("S3 staging path must start with s3://")
        parts = s3_path[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix

    def _connect(self) -> Connection:
        return sql.connect(
            server_hostname=self.settings.server_hostname,
            http_path=self.settings.http_path,
            access_token=self.settings.token,
        )

    def initialize_schema(self) -> None:
        """Creates the necessary tables and structures."""
        schema_path = Path(__file__).parent / "sql" / "databricks_schema.sql"
        with open(schema_path) as f:
            schema_sql = f.read()

        with self._connect() as connection:
            with connection.cursor() as cursor:
                # Databricks SQL connector does not support multiple statements in one call
                for statement in schema_sql.split(";"):
                    statement = statement.strip()
                    if statement:
                        logger.info(f"Executing schema statement: {statement[:100]}...")
                        cursor.execute(statement)
        logger.info("Databricks schema initialized successfully.")

    def bulk_load_to_staging(self, intermediate_dir: Path) -> int:
        """
        Uploads intermediate files to S3 and uses COPY INTO.

        :return: The total number of rows loaded.
        """
        total_rows_loaded = 0
        s3_uri = self.s3_uploader.upload_directory(intermediate_dir)
        logger.info(f"Intermediate files uploaded to {s3_uri}")

        with self._connect() as connection:
            with connection.cursor() as cursor:
                for table_base_name in self._get_table_names():
                    # Find all chunked files for the current table
                    for file_path in intermediate_dir.glob(f"{table_base_name}*.csv"):
                        s3_file_path = f"{s3_uri}/{file_path.name}"
                        staging_table = f"{table_base_name}_staging"

                        copy_sql = f"""
                        COPY INTO {staging_table}
                        FROM '{s3_file_path}'
                        FILEFORMAT = CSV
                        FORMAT_OPTIONS ('header' = 'false', 'nullValue' = '\\N')
                        COPY_OPTIONS ('mergeSchema' = 'true')
                        """
                        logger.info(
                            f"Executing COPY INTO for {staging_table} from {s3_file_path}..."
                        )
                        cursor.execute(copy_sql)
                        # The rowcount from the cursor gives us the number of
                        # affected rows.
                        if cursor.rowcount:
                            total_rows_loaded += cursor.rowcount
                            logger.info(f"Loaded {cursor.rowcount} rows.")
        logger.info(
            "Databricks bulk load complete. Total rows loaded: %d",
            total_rows_loaded,
        )
        return total_rows_loaded

    def pre_load_optimization(self, mode: Literal["full-load", "delta-load"]) -> None:
        pass

    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        """Merges data from staging tables to production tables."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if mode == "full-load":
                    logger.info("Performing full load from staging to production...")
                    for table in self._get_table_names():
                        logger.info(f"Truncating {table}...")
                        cursor.execute(f"TRUNCATE TABLE {table}")
                        logger.info(
                            f"Inserting data from {table}_staging to {table}..."
                        )
                        cursor.execute(
                            f"INSERT INTO {table} SELECT * FROM {table}_staging"
                        )
                elif mode == "delta-load":
                    logger.info("Performing delta load from staging to production...")
                    # This is a simplified merge for the main tables.
                    # A more robust implementation would handle updates and deletes.
                    for table in self._get_table_names():
                        pk = self._get_primary_key(table)
                        staging_table = f"{table}_staging"
                        merge_sql = f"""
                        MERGE INTO {table} AS target
                        USING {staging_table} AS source
                        ON target.{pk} = source.{pk}
                        WHEN NOT MATCHED THEN INSERT *
                        """
                        logger.info(f"Merging data into {table}...")
                        cursor.execute(merge_sql)

    def _get_table_names(self) -> list[str]:
        """Returns the list of tables to be loaded."""
        return [
            "spl_raw_documents",
            "products",
            "product_ndcs",
            "ingredients",
            "packaging",
            "marketing_status",
        ]

    def _get_primary_key(self, table: str) -> str:
        """Returns the primary key for a given table."""
        pk_map = {
            "spl_raw_documents": "document_id",
            "products": "document_id",
            "product_ndcs": "id",
            "ingredients": "id",
            "packaging": "id",
            "marketing_status": "id",
        }
        return pk_map[table]

    def post_load_cleanup(self, mode: Literal["full-load", "delta-load"]) -> None:
        pass

    def start_run(self, mode: Literal["full-load", "delta-load"]) -> int:
        """Creates a new entry in the ETL history table."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO etl_load_history (start_time, status, mode) VALUES (current_timestamp(), 'RUNNING', %s)",
                    (mode,),
                )
                # Databricks doesn't have a reliable last_insert_id, so we get the max.
                # This is not perfectly safe in highly concurrent environments.
                cursor.execute("SELECT MAX(run_id) FROM etl_load_history")
                row = cursor.fetchone()
                if row:
                    run_id: int = row[0]
                    return run_id
                raise RuntimeError("Could not retrieve run_id after starting run.")

    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:
        """Updates the ETL history table for the specified run."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE etl_load_history
                    SET end_time = current_timestamp(), status = %s, records_loaded = %s, error_log = %s
                    WHERE run_id = %s
                    """,
                    (status, records_loaded, error_log, run_id),
                )

    def get_processed_archives(self) -> set[str]:
        """Retrieves the set of already processed archive names."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT archive_name FROM etl_processed_archives")
                return {row[0] for row in cursor.fetchall()}

    def record_processed_archive(self, archive_name: str, checksum: str) -> None:
        """Records a successfully processed archive."""
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO etl_processed_archives (archive_name, archive_checksum, processed_timestamp) VALUES (%s, %s, current_timestamp())",
                    (archive_name, checksum),
                )
