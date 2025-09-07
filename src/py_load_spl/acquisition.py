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


def download_spl_archives() -> None:
    """
    Main function for F001: Data Acquisition.
    Downloads the smallest daily update file as a demonstration.
    """
    logger.info("Starting SPL data acquisition...")
    settings = get_settings()
    archive_list = get_archive_list(settings)

    # For demonstration, find and download one of the smallest daily archives
    target_archive_name = "dm_spl_daily_update_09022025.zip"
    target_archive = next((a for a in archive_list if a["name"] == target_archive_name), None)

    if target_archive:
        try:
            download_archive(target_archive, settings)
            logger.info("Successfully downloaded and verified %s.", target_archive_name)
        except Exception as e:
            logger.error("An error occurred during download: %s", e, exc_info=True)
    else:
        logger.warning("Could not find the target archive %s for download.", target_archive_name)

    logger.info("Data acquisition download step completed.")
