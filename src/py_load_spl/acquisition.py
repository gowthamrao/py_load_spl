import concurrent.futures
import hashlib
import logging
import re
from pathlib import Path

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
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import Settings, get_settings
from .db.base import DatabaseLoader
from .models import Archive

logger = logging.getLogger(__name__)


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def get_archive_list(settings: Settings) -> list[Archive]:
    """
    Scrapes the DailyMed SPL download page to get a list of all available archives.
    This function is decorated to be resilient to transient network errors.
    """
    logger.info("Fetching archive list from %s", settings.fda_source_url)
    # The try/except block is removed as tenacity will handle the exception
    # and raise a RetryError if all attempts fail.
    response = requests.get(str(settings.fda_source_url), timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "lxml")
    archives: list[Archive] = []

    # Find all list items in the download sections
    for li in soup.select("ul.download > li"):
        # Find the HTTPS link within the list item
        https_link = li.find("a", string=lambda text: text and "HTTPS" in text)
        if not https_link:
            continue

        href = https_link.get("href")  # type: ignore[attr-defined]
        if not href or not href.endswith(".zip"):
            continue

        # Find the checksum within the list item's text
        checksum_match = re.search(r"MD5 checksum:\s*([0-9a-fA-F]{32})", li.get_text())
        if checksum_match:
            archives.append(
                Archive(
                    name=href.split("/")[-1],
                    url=href,
                    checksum=checksum_match.group(1).strip(),
                )
            )

    if not archives:
        logger.warning(
            "Could not find any archives on the page. "
            "The page structure may have changed."
        )
    else:
        logger.info("Found %d archives to process.", len(archives))
    return archives


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def download_archive(archive: Archive, settings: Settings) -> Path:
    """
    Downloads a single archive file, verifies its checksum, and saves it.

    This function is decorated to be resilient to transient network errors.
    """
    download_dir = Path(settings.download_path)
    download_dir.mkdir(parents=True, exist_ok=True)
    file_path = download_dir / archive.name

    logger.info("Downloading %s to %s", archive.url, file_path)

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
    # Tenacity will handle retrying on requests.RequestException.
    # We still need to handle other potential errors, like checksum mismatches.
    try:
        with requests.get(archive.url, stream=True, timeout=300) as r:
            r.raise_for_status()  # Let tenacity catch this if it's a network error
            total_size = int(r.headers.get("content-length", 0))
            task_id = progress.add_task(
                "download", total=total_size, filename=archive.name
            )
            with open(file_path, "wb") as f, progress:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    md5.update(chunk)
                    progress.update(task_id, advance=len(chunk))

        calculated_checksum = md5.hexdigest()
        if calculated_checksum.lower() != archive.checksum.lower():
            msg = (
                f"Checksum mismatch for {archive.name}. "
                f"Expected {archive.checksum}, got {calculated_checksum}."
            )
            raise ValueError(msg)
        logger.info("Checksum verified for %s", archive.name)
        return file_path
    except ValueError as e:
        # This will catch the checksum error specifically. We don't want to retry this.
        logger.error("Data integrity error for %s: %s", archive.name, e)
        if file_path.exists():
            file_path.unlink()  # Delete corrupted file
        raise  # Re-raise the ValueError
    except Exception as e:
        # Catch any other unexpected errors during file handling
        logger.error(
            "An unexpected error occurred during download of %s: %s", archive.name, e
        )
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        raise


def download_spl_archives(loader: DatabaseLoader) -> list[Archive]:
    """
    Main function for F001: Data Acquisition.

    Performs a stateful, parallel download of SPL archives. It fetches the
    list of available archives, compares it against previously processed ones,
    and downloads only the new ones concurrently. If any downloads fail, it
    raises an ExceptionGroup containing all failures.

    Args:
        loader: An initialized database loader instance.

    Returns:
        A list of Archive objects that were successfully downloaded.

    Raises:
        ExceptionGroup: If one or more archives fail to download.
    """
    logger.info("Starting stateful SPL data acquisition...")
    settings = get_settings()

    try:
        processed_archives_names = loader.get_processed_archives()
    except Exception as e:
        logger.error(
            f"Could not connect to DB to get processed archives. Aborting. Error: {e}"
        )
        return []

    all_available_archives = get_archive_list(settings)
    if not all_available_archives:
        logger.warning("No archives found at source. Nothing to download.")
        return []

    all_available_archives_map = {a.name: a for a in all_available_archives}
    new_archive_names = (
        set(all_available_archives_map.keys()) - processed_archives_names
    )

    if not new_archive_names:
        logger.info("No new archives to download. Database is up to date.")
        return []

    archives_to_download = [
        all_available_archives_map[name] for name in sorted(new_archive_names)
    ]
    logger.info(f"Found {len(archives_to_download)} new archives to download.")

    downloaded_archives: list[Archive] = []
    exceptions = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=settings.max_workers
    ) as executor:
        future_to_archive = {
            executor.submit(download_archive, archive, settings): archive
            for archive in archives_to_download
        }

        for future in concurrent.futures.as_completed(future_to_archive):
            archive = future_to_archive[future]
            try:
                future.result()
                downloaded_archives.append(archive)
            except Exception as e:
                e.add_note(f"Archive: {archive.name}, URL: {archive.url}")
                exceptions.append(e)

    if exceptions:
        raise ExceptionGroup(
            f"Completed with {len(exceptions)} download error(s)", exceptions
        )

    logger.info(
        "Data acquisition step completed. Successfully downloaded %d files.",
        len(downloaded_archives),
    )
    return downloaded_archives


def download_all_archives(settings: Settings | None = None) -> list[Archive]:
    """
    Main function for downloading ALL SPL archives for a full load.

    This is a non-stateful, parallel download. It gets the full list of
    archives from the FDA source and attempts to download every single one
    concurrently. If any downloads fail, it raises an ExceptionGroup
    containing all failures.

    Returns:
        A list of Archive objects that were successfully downloaded.

    Raises:
        ExceptionGroup: If one or more archives fail to download.
    """
    logger.info("Starting download of all SPL data for full load...")
    if settings is None:
        settings = get_settings()

    all_available_archives = get_archive_list(settings)
    if not all_available_archives:
        logger.warning("No archives found at source. Nothing to download.")
        return []

    archives_to_download = sorted(all_available_archives, key=lambda a: a.name)
    logger.info(f"Found {len(archives_to_download)} total archives to download.")

    downloaded_archives: list[Archive] = []
    exceptions = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=settings.max_workers
    ) as executor:
        future_to_archive = {
            executor.submit(download_archive, archive, settings): archive
            for archive in archives_to_download
        }

        for future in concurrent.futures.as_completed(future_to_archive):
            archive = future_to_archive[future]
            try:
                future.result()
                downloaded_archives.append(archive)
            except Exception as e:
                e.add_note(f"Archive: {archive.name}, URL: {archive.url}")
                exceptions.append(e)

    if exceptions:
        raise ExceptionGroup(
            f"Completed with {len(exceptions)} download error(s)", exceptions
        )

    logger.info(
        "Full data acquisition step completed. Successfully downloaded %d of %d files.",
        len(downloaded_archives),
        len(all_available_archives),
    )
    return downloaded_archives
