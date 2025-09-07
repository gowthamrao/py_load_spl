import hashlib
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
import requests

from src.py_load_faers import acquisition

# A small, representative sample of the HTML from the FAERS download page.
MOCK_HTML_CONTENT = """
<html><body>
    <h1>FAERS Quarterly Data Extract Files</h1>
    <a href="/content/Exports/faers_ascii_2025q2.zip">ASCII</a>
    <a href="/content/Exports/faers_xml_2025q2.zip">XML</a>
    <a href="/content/Exports/faers_ascii_2025q1.zip">ASCII</a>
    <a href="/content/Exports/faers_xml_2025q1.zip">XML</a>
    <a href="/content/Exports/aers_ascii_2012q3.zip">Older ASCII</a>
    <a href="malformed.zip">Invalid Link</a>
</body></html>
"""


def test_list_available_archives(mocker):
    """
    Tests that the scraper correctly parses a mock HTML page.
    """
    # Mock the requests.get call
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.content = MOCK_HTML_CONTENT.encode("utf-8")
    mocker.patch("requests.get", return_value=mock_response)

    archives = list(acquisition.list_available_archives())

    assert len(archives) == 5

    # Check the first record
    assert archives[0]["quarter"] == "2025Q2"
    assert archives[0]["format"] == "ascii"
    assert archives[0]["url"].endswith("/content/Exports/faers_ascii_2025q2.zip")

    # Check the older 'aers' format
    assert archives[4]["quarter"] == "2012Q3"
    assert archives[4]["format"] == "ascii"
    assert archives[4]["url"].endswith("/content/Exports/aers_ascii_2012q3.zip")


def create_mock_zip() -> bytes:
    """Creates a simple, valid zip file in memory."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("test.txt", "hello world")
    return zip_buffer.getvalue()


def test_download_archive_success(mocker, tmp_path: Path):
    """
    Tests a successful download, including integrity check and checksum.
    """
    mock_zip_content = create_mock_zip()
    mock_url = "http://fake.host/test.zip"
    destination = tmp_path / "test.zip"

    # Mock the streaming download response
    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers = {"content-length": str(len(mock_zip_content))}
    mock_response.iter_content.return_value = [mock_zip_content]

    # The mock for requests.get needs to return a context manager
    mock_context_manager = mocker.MagicMock()
    mock_context_manager.__enter__.return_value = mock_response
    mocker.patch("requests.get", return_value=mock_context_manager)

    result = acquisition.download_archive(mock_url, destination)

    assert destination.exists()
    assert destination.read_bytes() == mock_zip_content

    expected_checksum = hashlib.sha256(mock_zip_content).hexdigest()
    assert result["sha256"] == expected_checksum


def test_download_archive_corrupt_zip(mocker, tmp_path: Path):
    """
    Tests that a corrupt zip file raises an error and is cleaned up.
    """
    corrupt_content = b"This is not a zip file"
    mock_url = "http://fake.host/corrupt.zip"
    destination = tmp_path / "corrupt.zip"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.headers = {"content-length": str(len(corrupt_content))}
    mock_response.iter_content.return_value = [corrupt_content]

    mock_context_manager = mocker.MagicMock()
    mock_context_manager.__enter__.return_value = mock_response
    mocker.patch("requests.get", return_value=mock_context_manager)

    with pytest.raises(zipfile.BadZipFile):
        acquisition.download_archive(mock_url, destination)

    # Assert that the corrupt file was deleted
    assert not destination.exists()


def test_download_archive_network_error(mocker, tmp_path: Path):
    """
    Tests that a network error (e.g., 404) raises the appropriate exception.
    """
    mock_url = "http://fake.host/notfound.zip"
    destination = tmp_path / "notfound.zip"

    mock_response = mocker.Mock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")

    mock_context_manager = mocker.MagicMock()
    mock_context_manager.__enter__.return_value = mock_response
    mocker.patch("requests.get", return_value=mock_context_manager)

    with pytest.raises(requests.exceptions.HTTPError):
        acquisition.download_archive(mock_url, destination)

    assert not destination.exists()
