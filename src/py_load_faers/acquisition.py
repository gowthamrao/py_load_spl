import logging
import re
from collections.abc import Generator
from urllib.parse import urljoin

import requests
from lxml import html

logger = logging.getLogger(__name__)

# The official page listing all quarterly data files
FAERS_DATA_PAGE_URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"


def list_available_archives() -> Generator[dict[str, str], None, None]:
    """
    Scrapes the FAERS data page to find all available quarterly archives.

    This function implements R1 from the FRD.

    Yields:
        A dictionary for each found archive with keys: 'quarter', 'url', 'format'.
        Example: {'quarter': '2025Q2', 'format': 'ascii', 'url': '...'}
    """
    logger.info(f"Scraping FAERS archive list from {FAERS_DATA_PAGE_URL}")
    try:
        response = requests.get(FAERS_DATA_PAGE_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Could not fetch FAERS data page: {e}")
        return

    tree = html.fromstring(response.content)
    # Use XPath to find all <a> tags with an href ending in .zip
    zip_links = tree.xpath("//a[contains(@href, '.zip')]")

    # Regex to extract year, quarter, and format from the URL
    # Handles both 'faers_ascii_2023Q4.zip' and 'aers_ascii_2011q3.zip'
    archive_pattern = re.compile(
        r"/(?P<prefix>faers|aers)_(?P<format>ascii|xml|sgml)_(?P<year>\d{4})(?P<qtr>q\d)\.zip",
        re.IGNORECASE,
    )

    for link in zip_links:
        href = link.get("href")
        if not href:
            continue

        match = archive_pattern.search(href)
        if match:
            data = match.groupdict()
            quarter_str = f"{data['year']}{data['qtr'].upper()}"
            full_url = urljoin(FAERS_DATA_PAGE_URL, href)

            yield {
                "quarter": quarter_str,
                "format": data["format"].lower(),
                "url": full_url,
            }
    logger.info(f"Finished scraping. Found {len(list(zip_links))} potential archives.")


import hashlib
import zipfile
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm.auto import tqdm


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def download_archive(url: str, destination_path: Path) -> dict[str, str]:
    """
    Downloads a file from a URL to a destination, with retries and a progress bar.
    Implements FRD R4 and R5.

    Args:
        url: The URL of the file to download.
        destination_path: The local path to save the file.

    Returns:
        A dictionary with the file path and its SHA-256 checksum.
    """
    logger.info(f"Downloading from {url} to {destination_path}...")
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            block_size = 8192  # 8 KB

            with tqdm(
                total=total_size, unit="iB", unit_scale=True, desc=destination_path.name
            ) as progress_bar:
                with open(destination_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=block_size):
                        progress_bar.update(len(chunk))
                        f.write(chunk)

            if total_size != 0 and progress_bar.n != total_size:
                raise IOError("Download incomplete: size mismatch.")

        logger.info("Download complete. Verifying integrity...")

        # R5.1: Verify the archive (ZIP) is not corrupted
        with zipfile.ZipFile(destination_path) as zf:
            if zf.testzip() is not None:
                raise zipfile.BadZipFile("Downloaded ZIP file is corrupt.")

        # R5.2: Generate a SHA-256 checksum for internal auditing
        sha256_hash = hashlib.sha256()
        with open(destination_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        checksum = sha256_hash.hexdigest()

        logger.info(f"Verification successful. SHA-256: {checksum}")
        return {"path": str(destination_path), "sha256": checksum}

    except (requests.RequestException, IOError, zipfile.BadZipFile) as e:
        logger.error(f"Download or verification for {url} failed: {e}")
        # Clean up partially downloaded file
        if destination_path.exists():
            destination_path.unlink()
        raise
