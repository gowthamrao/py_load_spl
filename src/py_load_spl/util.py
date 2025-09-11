import logging
import sys
import zipfile
from pathlib import Path

from pythonjsonlogger.json import JsonFormatter


def setup_logging(log_level: str, log_format: str) -> None:
    """
    Configures the root logger for the application.
    """
    logger = logging.getLogger()
    logger.setLevel(log_level.upper())

    # Remove any existing handlers to avoid duplicate logs
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)

    formatter: logging.Formatter
    if log_format.lower() == "json":
        formatter = JsonFormatter("%(asctime)s %(name)s %(levelname)s %(message)s")
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logging.getLogger(__name__).info(
        f"Logging configured. Level: {log_level}, Format: {log_format}"
    )


def unzip_archive(archive_path: Path, extract_to: Path) -> None:
    """
    Extracts a zip archive to a specified directory.

    Args:
        archive_path: The path to the zip file.
        extract_to: The directory where contents should be extracted.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Extracting '{archive_path.name}' to '{extract_to}'...")
    try:
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        logger.info(f"Successfully extracted '{archive_path.name}'.")
    except (zipfile.BadZipFile, FileNotFoundError) as e:
        logger.error(f"Failed to extract archive {archive_path}: {e}")
        raise
