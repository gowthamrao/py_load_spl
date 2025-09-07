import pytest
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_spl.cli import app
from py_load_spl.config import Settings, DatabaseSettings

runner = CliRunner()


def test_app_exists() -> None:
    """
    A very simple test to ensure the Typer app object can be imported.
    """
    assert app is not None


@pytest.mark.integration
def test_init_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the 'init' command runs without errors against a test container.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        # Create a settings object with the dynamic details from the container
        test_db_settings = DatabaseSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
            adapter="postgresql",
        )
        test_settings = Settings(db=test_db_settings)

        # Use monkeypatch to make the CLI use our test settings
        monkeypatch.setattr("py_load_spl.cli.get_settings", lambda: test_settings)

        # Run the command
        result = runner.invoke(app, ["init"])

        # Assert success
        assert result.exit_code == 0, f"CLI command failed with output:\n{result.stdout}"
        assert "Initializing database schema" in result.stdout
        assert "Schema initialization complete" in result.stdout


import hashlib
from pathlib import Path
import requests_mock

# A simplified HTML fixture mimicking the structure of the DailyMed page
HTML_FIXTURE = """
<html>
<body>
    <ul class="download">
        <li>
            dm_spl_daily_update_09022025.zip [ <a href="https://example.com/dm_spl_daily_update_09022025.zip">HTTPS</a> ]
            <ul><li>MD5 checksum: {checksum}</li></ul>
        </li>
    </ul>
</body>
</html>
"""


import logging


@pytest.mark.integration
def test_download_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Test the 'download' command runs and successfully downloads a mock file.
    """
    # Prepare mock data and settings
    mock_content = b"mock zip data"
    mock_checksum = hashlib.md5(mock_content).hexdigest()
    archive_name = "dm_spl_daily_update_09022025.zip"
    download_url = f"https://example.com/{archive_name}"

    # Use monkeypatch to override settings to use a temporary download path
    test_settings = Settings(download_path=str(tmp_path))
    monkeypatch.setattr("py_load_spl.acquisition.get_settings", lambda: test_settings)

    with caplog.at_level(logging.INFO):
        with requests_mock.Mocker() as m:
            # Mock the listing page
            m.get(
                str(test_settings.fda_source_url),
                text=HTML_FIXTURE.format(checksum=mock_checksum),
            )
            # Mock the file download
            m.get(
                download_url,
                content=mock_content,
                headers={"Content-Length": str(len(mock_content))},
            )

            # Run the command
            result = runner.invoke(app, ["download"])

    # Assert success
    assert result.exit_code == 0, f"CLI command failed with output:\n{result.output}"
    assert "Successfully downloaded and verified" in caplog.text
    assert archive_name in caplog.text

    # Verify the file was created
    expected_file = tmp_path / archive_name
    assert expected_file.exists()
    assert expected_file.read_bytes() == mock_content
