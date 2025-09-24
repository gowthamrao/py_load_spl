import concurrent.futures
import logging
import tempfile
from collections.abc import Generator
from concurrent.futures import as_completed
from pathlib import Path
from typing import Any

from .acquisition import download_spl_archives
from .config import DatabricksSettings, RedshiftSettings, Settings
from .db.base import DatabaseLoader
from .db.databricks import DatabricksLoader
from .db.postgres import PostgresLoader
from .db.redshift import RedshiftLoader
from .db.sqlite import SqliteLoader
from .parsing import parse_spl_file
from .transformation import CsvWriter, FileWriter, ParquetWriter, Transformer
from .util import unzip_archive

logger = logging.getLogger(__name__)


def get_db_loader(settings: Settings) -> DatabaseLoader:
    """Instantiates the correct database loader based on settings."""
    adapter = settings.db.adapter
    logger.info(f"Initializing database adapter: {adapter}")
    if adapter == "postgresql":
        return PostgresLoader(settings.db)
    elif adapter == "sqlite":
        return SqliteLoader(settings.db)
    elif adapter == "redshift":
        assert isinstance(settings.db, RedshiftSettings), (
            "DB adapter is 'redshift' but settings are not RedshiftSettings"
        )
        return RedshiftLoader(settings.db, settings.s3)
    elif adapter == "databricks":
        assert isinstance(settings.db, DatabricksSettings), (
            "DB adapter is 'databricks' but settings are not DatabricksSettings"
        )
        return DatabricksLoader(settings.db)
    else:
        # This path should be unreachable due to Pydantic validation
        logger.error(f"Unsupported DB adapter '{adapter}'")
        raise ValueError(f"Unsupported DB adapter '{adapter}'")


def get_file_writer(settings: Settings, output_dir: Path) -> FileWriter:
    """Instantiates the correct file writer based on settings."""
    if settings.intermediate_format == "parquet":
        logger.info("Using Parquet format for intermediate files.")
        return ParquetWriter(output_dir)
    elif settings.intermediate_format == "csv":
        logger.info("Using CSV format for intermediate files.")
        return CsvWriter(output_dir)
    else:
        # This case should be prevented by Pydantic validation, but as a safeguard:
        logger.error(
            f"Unsupported intermediate format '{settings.intermediate_format}'"
        )
        raise ValueError(
            f"Unsupported intermediate format '{settings.intermediate_format}'"
        )


def _quarantine_and_parse_in_parallel(
    xml_files: list[Path],
    settings: Settings,
    executor: concurrent.futures.ProcessPoolExecutor,
) -> Generator[dict[str, Any], None, None]:
    """
    Parses a list of XML files in parallel, quarantining any file that fails.
    Yields successfully parsed data dictionaries.
    """
    futures = {executor.submit(parse_spl_file, file): file for file in xml_files}
    quarantined_count = 0

    for future in as_completed(futures):
        source_file_path = futures[future]
        try:
            yield future.result()
        except Exception as e:
            quarantine_dir = Path(settings.quarantine_path)
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            target_path = quarantine_dir / source_file_path.name
            if source_file_path.exists():
                source_file_path.rename(target_path)
                quarantined_count += 1
                logging.warning(
                    "Moved corrupted file %s to %s due to parsing error: %s",
                    source_file_path.name,
                    target_path,
                    e,
                )
            else:
                logging.warning(
                    "Could not quarantine %s as it was already moved or deleted.",
                    source_file_path.name,
                )

    if quarantined_count > 0:
        logger.warning(f"Quarantined {quarantined_count} file(s).")


def run_full_load(settings: Settings, source: Path) -> None:
    """The core logic for a full data load from a given source directory."""
    logger.info(f"Starting full data load from '{source}'...")
    loader = get_db_loader(settings)
    run_id = None
    try:
        run_id = loader.start_run(mode="full-load")
        with tempfile.TemporaryDirectory() as temp_dir_str:
            output_dir = Path(temp_dir_str)
            logger.info(f"Intermediate files will be stored in: {output_dir}")

            writer = get_file_writer(settings, output_dir)

            logger.info("Step 1: Finding XML files...")
            xml_files = list(source.glob("**/*.xml"))
            if not xml_files:
                logger.warning("No XML files found in the source. Aborting.")
                if run_id:
                    loader.end_run(run_id, "SUCCESS", 0, None)
                return
            logger.info(f"Found {len(xml_files)} XML files to process.")

            logger.info(
                f"Step 2: Parsing and Transforming in parallel (max_workers={settings.max_workers})..."
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = _quarantine_and_parse_in_parallel(
                    xml_files, settings, executor
                )
                transformer = Transformer(writer=writer)
                stats = transformer.transform_stream(parsed_data_stream)

            logger.info("Parsing and Transformation complete.")

            logger.info("Step 3: Loading data into database...")
            loader.pre_load_optimization(mode="full-load")
            loaded_count = loader.bulk_load_to_staging(output_dir)

            # F002.1 Data Integrity Validation
            transformed_count = sum(stats.values())
            if loaded_count != transformed_count:
                mismatch_msg = (
                    f"Data integrity check failed! Transformed records ({transformed_count}) "
                    f"does not match loaded records ({loaded_count})."
                )
                logger.error(mismatch_msg)
                raise RuntimeError(mismatch_msg)
            logger.info(
                "Data integrity check passed: %d transformed, %d loaded.",
                transformed_count,
                loaded_count,
            )

            loader.merge_from_staging("full-load")
            loader.post_load_cleanup(mode="full-load")
            logger.info("Database loading complete.")

        if run_id:
            loader.end_run(run_id, "SUCCESS", loaded_count, None)
        logger.info("Full load process finished successfully.")
    except Exception as e:
        logger.error(
            f"An error occurred during the full load process: {e}", exc_info=True
        )
        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise


def run_delta_load(settings: Settings) -> None:
    """The core logic for an incremental (delta) load from the FDA source."""
    logger.info("Starting delta data load from FDA source...")
    loader = get_db_loader(settings)
    run_id = None
    try:
        run_id = loader.start_run(mode="delta-load")
        logger.info("Step 1: Checking for and downloading new archives...")
        downloaded_archives = download_spl_archives(loader)
        if not downloaded_archives:
            logger.info("No new archives found. Database is up-to-date.")
            if run_id:
                loader.end_run(run_id, "SUCCESS", 0, None)
            return
        logger.info(f"Downloaded {len(downloaded_archives)} new archive(s).")
        with (
            tempfile.TemporaryDirectory() as xml_temp_dir_str,
            tempfile.TemporaryDirectory() as intermediate_dir_str,
        ):
            xml_temp_dir = Path(xml_temp_dir_str)
            intermediate_dir = Path(intermediate_dir_str)
            writer = get_file_writer(settings, intermediate_dir)
            logger.info(f"Step 2: Extracting XML files to {xml_temp_dir}...")
            for archive in downloaded_archives:
                archive_path = Path(settings.download_path) / archive.name
                unzip_archive(archive_path, xml_temp_dir)
            logger.info(f"Step 3: Finding XML files in {xml_temp_dir}...")
            xml_files = list(xml_temp_dir.glob("**/*.xml"))
            logger.info(f"Found {len(xml_files)} XML files to process.")
            logger.info(
                f"Step 4: Parsing and Transforming in parallel (max_workers={settings.max_workers})..."
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = _quarantine_and_parse_in_parallel(
                    xml_files, settings, executor
                )
                transformer = Transformer(writer=writer)
                stats = transformer.transform_stream(parsed_data_stream)
            logger.info("Parsing and Transformation complete.")
            logger.info("Step 5: Loading data into database...")
            loader.pre_load_optimization(mode="delta-load")
            loaded_count = loader.bulk_load_to_staging(intermediate_dir)
            transformed_count = sum(stats.values())
            if loaded_count != transformed_count:
                mismatch_msg = (
                    f"Data integrity check failed! Transformed records ({transformed_count}) "
                    f"does not match loaded records ({loaded_count})."
                )
                logger.error(mismatch_msg)
                raise RuntimeError(mismatch_msg)
            logger.info(
                "Data integrity check passed: %d transformed, %d loaded.",
                transformed_count,
                loaded_count,
            )
            loader.merge_from_staging("delta-load")
            loader.post_load_cleanup(mode="delta-load")
            logger.info("Database loading complete.")
            logger.info("Step 6: Recording processed archives in database...")
            for archive in downloaded_archives:
                loader.record_processed_archive(archive.name, archive.checksum)
        if run_id:
            loader.end_run(run_id, "SUCCESS", loaded_count, None)
        logger.info("Delta load process finished successfully.")
    except Exception as e:
        if isinstance(e, ExceptionGroup):
            logger.error(
                "Delta load failed due to one or more download errors.", exc_info=True
            )
        else:
            logger.error(
                f"Delta load failed with an unexpected error: {e}", exc_info=True
            )

        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise
