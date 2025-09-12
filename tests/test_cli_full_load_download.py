import hashlib
import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests_mock
from typer.testing import CliRunner

from py_load_spl.cli import app

# Fixture for the CLI runner
runner = CliRunner()


# Create a dummy zip file in memory
@pytest.fixture(scope="session")
def dummy_zip_content() -> bytes:
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zf:
        zf.writestr("test.xml", "<root>test</root>")
    zip_buffer.seek(0)
    return zip_buffer.read()


@pytest.fixture(scope="session")
def dummy_zip_md5(dummy_zip_content: bytes) -> str:
    return hashlib.md5(dummy_zip_content).hexdigest()


@patch("py_load_spl.cli.get_db_loader", MagicMock())
def test_full_load_downloads_when_no_source_is_provided(
    requests_mock: requests_mock.Mocker,
    dummy_zip_content: bytes,
    dummy_zip_md5: str,
) -> None:
    """
    Verify that `full-load` without --source triggers the download process.
    """
    # The get_db_loader is already mocked by the decorator.

    # Mock the FDA source page with the correct checksum
    mock_html_content = f"""
    <html><body><ul class="download">
        <li><a href="https://example.com/spl_archive_1.zip">HTTPS</a> - MD5 checksum: {dummy_zip_md5}</li>
        <li><a href="https://example.com/spl_archive_2.zip">HTTPS</a> - MD5 checksum: {dummy_zip_md5}</li>
    </ul></body></html>
    """
    settings_url = (
        "https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm"
    )
    requests_mock.get(settings_url, text=mock_html_content)

    # Mock the archive downloads
    requests_mock.get(
        "https://example.com/spl_archive_1.zip", content=dummy_zip_content
    )
    requests_mock.get(
        "https://example.com/spl_archive_2.zip", content=dummy_zip_content
    )

    # Mock the core logic function to check if it's called
    with patch("py_load_spl.cli.run_full_load") as mock_run_full_load:
        result = runner.invoke(
            app, ["--log-format", "text", "full-load"], catch_exceptions=False
        )

        assert result.exit_code == 0
        assert "Downloading all archives" in result.stdout
        assert "Extracting 2 archives" in result.stdout

        # Verify that the core logic was called
        mock_run_full_load.assert_called_once()
        # The second argument is the `source` Path object
        call_args = mock_run_full_load.call_args[0]
        assert isinstance(call_args[1], Path)


@patch("py_load_spl.cli.get_db_loader", MagicMock())
def test_full_load_uses_source_when_provided(tmp_path: Path) -> None:
    """
    Verify that `full-load` with --source uses the provided path and does not download.
    """
    # Create a dummy XML file in the temp source directory
    source_dir = tmp_path / "xmls"
    source_dir.mkdir()
    (source_dir / "test.xml").write_text("<root></root>")

    with patch("py_load_spl.cli.run_full_load") as mock_run_full_load:
        result = runner.invoke(app, ["full-load", "--source", str(source_dir)])

        assert result.exit_code == 0
        assert "Downloading all archives" not in result.stdout

        mock_run_full_load.assert_called_once()
        call_args = mock_run_full_load.call_args[0]
        assert call_args[1] == source_dir
