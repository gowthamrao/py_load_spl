import logging
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pythonjsonlogger.json import JsonFormatter

from py_load_spl.util import setup_logging, unzip_archive


def test_unzip_archive_success(tmp_path: Path) -> None:
    """Tests that a valid zip file is extracted correctly."""
    zip_path = tmp_path / "test.zip"
    extract_to = tmp_path / "extracted"
    extract_to.mkdir()

    # Create a dummy file inside a zip archive
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("test.txt", "hello")

    unzip_archive(zip_path, extract_to)

    assert (extract_to / "test.txt").exists()
    assert (extract_to / "test.txt").read_text() == "hello"


def test_unzip_archive_bad_zipfile(tmp_path: Path) -> None:
    """Tests that a BadZipFile error is caught and raised."""
    bad_zip_path = tmp_path / "bad.zip"
    bad_zip_path.write_text("this is not a zip file")
    extract_to = tmp_path / "extracted"

    with pytest.raises(zipfile.BadZipFile):
        unzip_archive(bad_zip_path, extract_to)


def test_unzip_archive_file_not_found(tmp_path: Path) -> None:
    """Tests that a FileNotFoundError is caught and raised."""
    non_existent_path = tmp_path / "non_existent.zip"
    extract_to = tmp_path / "extracted"

    with pytest.raises(FileNotFoundError):
        unzip_archive(non_existent_path, extract_to)


@pytest.mark.parametrize(
    "log_format, formatter_class",
    [
        ("json", JsonFormatter),
        ("text", logging.Formatter),
        ("TEXT", logging.Formatter),  # Test case-insensitivity
    ],
)
def test_setup_logging(
    monkeypatch: pytest.MonkeyPatch,
    log_format: str,
    formatter_class: type[logging.Formatter],
) -> None:
    """Tests that logging is configured correctly for different formats."""
    mock_root_logger = MagicMock()
    # When getLogger is called with no name, it's the root logger
    monkeypatch.setattr(logging, "getLogger", lambda name=None: mock_root_logger)

    setup_logging("INFO", log_format)

    mock_root_logger.setLevel.assert_called_with("INFO")
    # Check that a handler was added
    assert mock_root_logger.addHandler.call_count == 1
    # Check that the handler has a formatter of the correct type
    handler_added = mock_root_logger.addHandler.call_args[0][0]
    assert isinstance(handler_added.formatter, formatter_class)
