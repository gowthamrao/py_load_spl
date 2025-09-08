import hashlib
import logging
import re
from pathlib import Path
from typing import List, TypedDict

import requests
from bs4 import BeautifulSoup
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from py_load_spl.config import Settings, get_settings
from py_load_spl.db.postgres import PostgresLoader

logger = logging.getLogger(__name__)


class Archive(TypedDict):
    """Represents a downloadable SPL archive file."""

    name: str
    url: str
    checksum: str


def get_archive_list(settings: Settings) -> List[Archive]:
    """
    Scrapes the DailyMed SPL download page to get a list of all available archives.
    """
    logger.info("Fetching archive list from %s", settings.fda_source_url)
    try:
        response = requests.get(str(settings.fda_source_url), timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to fetch archive list: %s", e)
        raise

    soup = BeautifulSoup(response.content, "lxml")
    archives: List[Archive] = []

    # Find all list items in the download sections
    for li in soup.select("ul.download > li"):
        # Find the HTTPS link within the list item
        https_link = li.find("a", string=lambda text: text and "HTTPS" in text)
        if not https_link:
            continue

        href = https_link.get("href")
        if not href or not href.endswith(".zip"):
            continue

        # Find the checksum within the list item's text
        checksum_match = re.search(r"MD5 checksum:\s*([0-9a-fA-F]{32})", li.get_text())
        if checksum_match:
            archives.append(
                {
                    "name": href.split("/")[-1],
                    "url": href,
                    "checksum": checksum_match.group(1).strip(),
                }
            )

    if not archives:
        logger.warning("Could not find any archives on the page. The page structure may have changed.")
    else:
        logger.info("Found %d archives to process.", len(archives))
    return archives


def download_archive(archive: Archive, settings: Settings) -> Path:
    """
    Downloads a single archive file, verifies its checksum, and saves it.
    """
    download_dir = Path(settings.download_path)
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / archive["name"]

    logger.info("Downloading %s to %s", archive["url"], file_path)

    progress = Progress(
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    md5 = hashlib.md5()
    try:
        with requests.get(archive["url"], stream=True, timeout=300) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            task_id = progress.add_task("download", total=total_size, filename=archive["name"])
            with open(file_path, "wb") as f, progress:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    md5.update(chunk)
                    progress.update(task_id, advance=len(chunk))

        calculated_checksum = md5.hexdigest()
        if calculated_checksum.lower() != archive["checksum"].lower():
            file_path.unlink()  # Delete corrupted file
            raise ValueError(
                f"Checksum mismatch for {archive['name']}. "
                f"Expected {archive['checksum']}, got {calculated_checksum}."
            )
        logger.info("Checksum verified for %s", archive["name"])
        return file_path
    except (requests.RequestException, ValueError) as e:
        logger.error("Failed to download or verify %s: %s", archive["name"], e)
        # Clean up partial download if it exists
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        raise


def download_spl_archives() -> List[Archive]:
    """
    Main function for F001: Data Acquisition.

    Performs a stateful download of SPL archives. It fetches the list of
    available archives, compares it against previously processed archives
    stored in the database, and downloads only the new ones.

    Returns:
        A list of Archive objects that were newly downloaded.
    """
    logger.info("Starting stateful SPL data acquisition...")
    settings = get_settings()

    # Instantiate loader to get state from the database.
    # In a larger application, this might be handled by dependency injection.
    loader = PostgresLoader(settings.db)
    try:
        processed_archives_names = loader.get_processed_archives()
    except Exception as e:
        logger.error(
            f"Could not connect to DB to get processed archives. Aborting. Error: {e}"
        )
        return []

    # Get all available archives from the source
    all_available_archives = get_archive_list(settings)
    if not all_available_archives:
        logger.warning("No archives found at source. Nothing to download.")
        return []

    # Determine which archives are new
    all_available_archives_map = {a["name"]: a for a in all_available_archives}
    new_archive_names = set(all_available_archives_map.keys()) - processed_archives_names

    if not new_archive_names:
        logger.info("No new archives to download. Database is up to date.")
        return []

    logger.info(f"Found {len(new_archive_names)} new archives to download.")

    # Download each new archive
    downloaded_archives: List[Archive] = []
    # Sort for deterministic download order, useful for testing/logging
    for name in sorted(list(new_archive_names)):
        archive_to_download = all_available_archives_map[name]
        try:
            download_archive(archive_to_download, settings)
            downloaded_archives.append(archive_to_download)
        except Exception as e:
            logger.error(
                f"Failed to download archive {name}. Skipping. Error: {e}", exc_info=True
            )
            # Continue to the next file
            continue

    logger.info(
        f"Data acquisition step completed. Downloaded {len(downloaded_archives)} files."
    )
    return downloaded_archives
