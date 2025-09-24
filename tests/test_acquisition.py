import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import requests_mock
from tenacity import RetryError

from py_load_spl.acquisition import (
    download_all_archives,
    download_archive,
    download_spl_archives,
    get_archive_list,
)
from py_load_spl.config import Settings
from py_load_spl.models import Archive

# This is a sample of the HTML structure from the real download page.
SAMPLE_HTML_VALID = f"""
<html>
<body>
  <div class="results-box">
    <ul class="download">
      <li>
        <a href="https://example.com/part1.zip">Part 1 (spls) - HTTPS</a>
        <br />MD5 checksum: 11111111111111111111111111111111
      </li>
      <li>
        <a href="https://example.com/part2.zip">Part 2 (spls) - HTTPS</a>
        <br />MD5 checksum: {hashlib.md5(b"data").hexdigest()}
      </li>
    </ul>
  </div>
</body>
</html>
"""

SAMPLE_HTML_MALFORMED = """
<html>
<body>
  <ul class="download">
      <li>Item without a link.</li>
      <li><a href="/wrong-path">A link that is not a zip file</a></li>
      <li><a href="https://example.com/no_checksum.zip">HTTPS</a><br />No checksum here</li>
  </ul>
</body>
</html>
"""


@pytest.fixture
def mock_settings(tmp_path: Path) -> Settings:
    """Provides a Settings instance with a temporary download path."""
    return Settings(download_path=str(tmp_path))


def test_get_archive_list(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests that the archive list is scraped correctly from the HTML."""
    requests_mock.get(str(mock_settings.fda_source_url), text=SAMPLE_HTML_VALID)
    archives = get_archive_list(mock_settings)
    assert len(archives) == 2
    assert archives[0].name == "part1.zip"
    assert archives[0].checksum == "11111111111111111111111111111111"
    assert archives[1].url == "https://example.com/part2.zip"


def test_get_archive_list_request_error(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests that a network error during list fetch is handled and raises RetryError
    after all attempts are exhausted.
    """
    requests_mock.get(
        str(mock_settings.fda_source_url),
        exc=requests.RequestException("Network Error"),
    )
    # After the tenacity decorator, this should raise RetryError, not the
    # original exception
    with pytest.raises(RetryError):
        get_archive_list(mock_settings)


def test_get_archive_list_malformed_page(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests that the scraper handles malformed or unexpected HTML gracefully."""
    requests_mock.get(str(mock_settings.fda_source_url), text=SAMPLE_HTML_MALFORMED)
    archives = get_archive_list(mock_settings)
    assert len(archives) == 0


def test_get_archive_list_no_archives_found(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests the case where the page is valid but contains no archives."""
    requests_mock.get(str(mock_settings.fda_source_url), text="<html></html>")
    archives = get_archive_list(mock_settings)
    assert len(archives) == 0


def test_get_archive_list_retries_on_transient_error(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests that the retry decorator correctly handles transient errors and eventually
    succeeds.
    """
    matcher = requests_mock.get(
        str(mock_settings.fda_source_url),
        [
            {"status_code": 503, "text": "Service Unavailable"},
            {"status_code": 200, "text": SAMPLE_HTML_VALID},
        ],
    )
    archives = get_archive_list(mock_settings)
    assert len(archives) == 2  # The call should eventually succeed
    assert matcher.call_count == 2  # It should have been called twice


def test_download_archive_success(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests a successful download and checksum verification."""
    content = b"hello"
    archive = Archive(
        name="test.zip",
        url="https://example.com/test.zip",
        checksum=hashlib.md5(content).hexdigest(),
    )
    requests_mock.get(
        "https://example.com/test.zip",
        content=content,
        headers={"Content-Length": str(len(content))},
    )
    file_path = download_archive(archive, mock_settings)
    assert file_path.exists()
    assert file_path.read_bytes() == content


def test_download_archive_checksum_mismatch(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests that a checksum mismatch raises a ValueError and cleans up the file."""
    archive = Archive(
        name="test.zip",
        url="https://example.com/test.zip",
        checksum="11111111111111111111111111111111",  # Incorrect checksum
    )
    requests_mock.get("https://example.com/test.zip", content=b"world")
    file_path = Path(mock_settings.download_path) / archive.name
    with pytest.raises(ValueError, match="Checksum mismatch"):
        download_archive(archive, mock_settings)
    assert not file_path.exists()


def test_download_archive_network_error(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests that a network error during download raises RetryError and cleans up.
    """
    archive = Archive(
        name="test.zip",
        url="https://example.com/test.zip",
        checksum="whatever",
    )
    requests_mock.get(
        "https://example.com/test.zip",
        exc=requests.RequestException("Connection timed out"),
    )
    file_path = Path(mock_settings.download_path) / archive.name
    # The decorator will now raise RetryError
    with pytest.raises(RetryError):
        download_archive(archive, mock_settings)
    # The cleanup logic is now inside the function's own try/except block
    assert not file_path.exists()


def test_download_spl_archives_db_error(mock_settings: Settings) -> None:
    """Tests that the process aborts gracefully if the DB is down."""
    mock_loader = MagicMock()
    mock_loader.get_processed_archives.side_effect = Exception("DB is offline")
    result = download_spl_archives(mock_loader)
    assert result == []


def test_download_spl_archives_no_archives_at_source(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests when the scraper finds no archives."""
    mock_loader = MagicMock()
    mock_loader.get_processed_archives.return_value = set()
    requests_mock.get(str(mock_settings.fda_source_url), text="<html></html>")
    result = download_spl_archives(mock_loader)
    assert result == []


def test_download_all_archives_single_failure_raises_exception_group(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests that failed downloads now raise an ExceptionGroup containing the failures.
    """
    requests_mock.get(str(mock_settings.fda_source_url), text=SAMPLE_HTML_VALID)
    requests_mock.get(
        "https://example.com/part1.zip",
        exc=requests.RequestException("Download failed"),
    )
    requests_mock.get(
        "https://example.com/part2.zip",
        content=b"data",
        headers={"Content-Length": "4"},
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        download_all_archives(mock_settings)

    # The successful download should not be returned, an exception is raised instead
    assert len(exc_info.value.exceptions) == 1
    inner_exc = exc_info.value.exceptions[0]
    # The RetryError is wrapped by our function's exception handling
    assert isinstance(inner_exc, RetryError)
    assert "part1.zip" in inner_exc.__notes__[0]


def test_download_archive_io_error(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests that an IOError during file write cleans up the partial file."""
    archive = Archive(
        name="test.zip", url="https://example.com/test.zip", checksum="abc"
    )
    requests_mock.get("https://example.com/test.zip", content=b"data")
    file_path = Path(mock_settings.download_path) / archive.name

    with patch("builtins.open", side_effect=OSError("Disk full")):
        with pytest.raises(IOError):
            download_archive(archive, mock_settings)

    # Even with the mock, the initial file path might be created before the error.
    # The important part is that the logic inside the except block is triggered.
    # A more robust test might involve patching Path.unlink to check it was called.
    assert not file_path.exists()


def test_download_spl_archives_single_failure_raises_exception_group(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests that a failed download during a delta load raises an ExceptionGroup.
    """
    requests_mock.get(str(mock_settings.fda_source_url), text=SAMPLE_HTML_VALID)
    mock_loader = MagicMock()
    mock_loader.get_processed_archives.return_value = set()

    requests_mock.get(
        "https://example.com/part1.zip",
        exc=requests.RequestException("Download failed"),
    )
    requests_mock.get(
        "https://example.com/part2.zip",
        content=b"data",
        headers={"Content-Length": "4"},
    )

    with pytest.raises(ExceptionGroup) as exc_info:
        download_spl_archives(mock_loader)

    assert len(exc_info.value.exceptions) == 1
    inner_exc = exc_info.value.exceptions[0]
    assert isinstance(inner_exc, RetryError)
    assert "part1.zip" in inner_exc.__notes__[0]


# A separate HTML sample for success tests with correct checksums
SAMPLE_HTML_FOR_SUCCESS_TEST = f"""
<html>
<body>
  <div class="results-box">
    <ul class="download">
      <li>
        <a href="https://example.com/part1.zip">Part 1 (spls) - HTTPS</a>
        <br />MD5 checksum: {hashlib.md5(b"data1").hexdigest()}
      </li>
      <li>
        <a href="https://example.com/part2.zip">Part 2 (spls) - HTTPS</a>
        <br />MD5 checksum: {hashlib.md5(b"data2").hexdigest()}
      </li>
    </ul>
  </div>
</body>
</html>
"""


def test_download_all_archives_success(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """Tests that all archives are downloaded successfully in parallel."""
    requests_mock.get(
        str(mock_settings.fda_source_url), text=SAMPLE_HTML_FOR_SUCCESS_TEST
    )
    requests_mock.get(
        "https://example.com/part1.zip",
        content=b"data1",
        headers={"Content-Length": "5"},
    )
    requests_mock.get(
        "https://example.com/part2.zip",
        content=b"data2",
        headers={"Content-Length": "5"},
    )

    downloaded = download_all_archives(mock_settings)
    assert len(downloaded) == 2
    assert {a.name for a in downloaded} == {"part1.zip", "part2.zip"}


def test_download_spl_archives_success(
    mock_settings: Settings, requests_mock: requests_mock.Mocker
) -> None:
    """
    Tests a successful delta download of multiple new archives.
    """
    requests_mock.get(
        str(mock_settings.fda_source_url), text=SAMPLE_HTML_FOR_SUCCESS_TEST
    )
    mock_loader = MagicMock()
    # Pretend no archives have been processed before
    mock_loader.get_processed_archives.return_value = set()

    requests_mock.get(
        "https://example.com/part1.zip",
        content=b"data1",
        headers={"Content-Length": "5"},
    )
    requests_mock.get(
        "https://example.com/part2.zip",
        content=b"data2",
        headers={"Content-Length": "5"},
    )

    downloaded = download_spl_archives(mock_loader)
    assert len(downloaded) == 2
    assert {a.name for a in downloaded} == {"part1.zip", "part2.zip"}


def test_download_all_archives_no_settings(
    requests_mock: requests_mock.Mocker,
) -> None:
    """Tests that get_settings() is called if no settings are provided."""
    with patch("py_load_spl.acquisition.get_settings") as mock_get_settings:
        mock_settings_instance = Settings()
        mock_get_settings.return_value = mock_settings_instance
        requests_mock.get(
            str(mock_settings_instance.fda_source_url), text="<html></html>"
        )
        download_all_archives()
        mock_get_settings.assert_called_once()
