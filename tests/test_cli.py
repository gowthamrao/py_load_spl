import hashlib
import logging
import zipfile
from pathlib import Path

import psycopg2
import pytest
import requests_mock
from pytest_mock import MockerFixture
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer
from typer.testing import CliRunner

from py_load_spl.cli import app
from py_load_spl.config import DatabaseSettings, PostgresSettings, Settings

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
    container = PostgresContainer("postgres:16-alpine")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres:
        # Create a settings object with the dynamic details from the container
        test_db_settings = PostgresSettings(
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
        assert result.exit_code == 0, (
            f"CLI command failed with output:\n{result.stdout}"
        )
        assert "Initializing database schema" in result.stdout
        assert "Schema initialization complete" in result.stdout


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


class MockLoader:
    def __init__(self, db_settings: DatabaseSettings | None):
        # We accept the settings but ignore them for the mock.
        pass

    def get_processed_archives(self) -> set[str]:
        # Return an empty set to simulate no archives being processed yet
        return set()

    def start_run(self, mode: str) -> int:
        return 1

    def end_run(
        self,
        run_id: int,
        status: str,
        records_loaded: int = 0,
        error_log: str | None = None,
    ) -> None:
        pass

    def record_processed_archive(self, name: str, checksum: str) -> None:
        pass


@pytest.fixture
def mock_db_loader(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fixture to mock the PostgresLoader to avoid real DB connections."""
    monkeypatch.setattr(
        "py_load_spl.main.get_db_loader",
        lambda settings: MockLoader(settings.db),
    )


# def test_download_command(
#     monkeypatch: pytest.MonkeyPatch,
#     tmp_path: Path,
#     caplog: pytest.LogCaptureFixture,
#     mock_db_loader: None,  # Activate the fixture
# ) -> None:
#     """
#     Test the 'download' command runs and successfully downloads a mock file.
#     """
#     # Prepare mock data and settings
#     mock_content = b"mock zip data"
#     mock_checksum = hashlib.md5(mock_content).hexdigest()
#     archive_name = "dm_spl_daily_update_09022025.zip"
#     download_url = f"https://example.com/{archive_name}"
#
#     # Use monkeypatch to override settings to use a temporary download path
#     test_settings = Settings(download_path=str(tmp_path))
#     monkeypatch.setattr("py_load_spl.acquisition.get_settings", lambda: test_settings)
#
#     with caplog.at_level(logging.INFO):
#         with requests_mock.Mocker() as m:
#             # Mock the listing page
#             m.get(
#                 str(test_settings.fda_source_url),
#                 text=HTML_FIXTURE.format(checksum=mock_checksum),
#             )
#             # Mock the file download
#             m.get(
#                 download_url,
#                 content=mock_content,
#                 headers={"Content-Length": str(len(mock_content))},
#             )
#
#             # Run the command
#             result = runner.invoke(app, ["download"])
#
#     # Assert success
#     assert result.exit_code == 0, f"CLI command failed with output:\n{result.output}"
#     assert "Data acquisition step completed. Downloaded 1 files." in caplog.text
#     assert archive_name in caplog.text
#
#     # Verify the file was created
#     expected_file = tmp_path / archive_name
#     assert expected_file.exists()
#     assert expected_file.read_bytes() == mock_content


def test_delta_load_no_new_archives(
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that delta-load handles the case where no new archives are found.
    """
    # Mock the functions that would perform external actions
    mock_loader_instance = MockLoader(None)
    mocker.patch.object(mock_loader_instance, "start_run", return_value=1)
    mocker.patch.object(mock_loader_instance, "end_run")

    monkeypatch.setattr(
        "py_load_spl.main.get_db_loader", lambda settings: mock_loader_instance
    )
    monkeypatch.setattr(
        "py_load_spl.main.download_spl_archives", lambda loader: []
    )  # No new archives

    with caplog.at_level(logging.INFO):
        result = runner.invoke(app, ["delta-load"])

    assert result.exit_code == 0
    assert "No new archives found" in result.stdout


SAMPLE_XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250909" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Test Drug</name>
        <formCode code="C42916" displayName="TABLET" />
        <asEntityWithGeneric>
          <genericMedicine>
            <name>TESTMED</name>
          </genericMedicine>
        </asEntityWithGeneric>
      </manufacturedProduct>
      <manufacturer>
        <name>Test Corp</name>
      </manufacturer>
    </manufacturedProduct>
  </subject>
</document>
"""


@pytest.mark.integration
def test_delta_load_end_to_end(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    An end-to-end integration test for the delta-load command.
    - Mocks the network calls to the FDA website.
    - Uses a real PostgreSQL database via testcontainers.
    - Verifies that data is downloaded, unzipped, parsed, and loaded correctly.
    """
    # 1. Prepare mock archive and settings
    archive_name = "dm_spl_daily_update_09092025.zip"

    # Create a dummy XML file and zip it
    xml_file = tmp_path / "test_spl.xml"
    xml_file.write_text(SAMPLE_XML_CONTENT)
    zip_file = tmp_path / archive_name
    with zipfile.ZipFile(zip_file, "w") as zf:
        zf.write(xml_file, arcname="test_spl.xml")

    mock_content = zip_file.read_bytes()
    mock_checksum = hashlib.md5(mock_content).hexdigest()

    # 2. Set up the test container and settings
    container = PostgresContainer("postgres:16-alpine")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres:
        test_db_settings = PostgresSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
            adapter="postgresql",
        )
        # Point download path to our temp dir
        test_settings = Settings(db=test_db_settings, download_path=str(tmp_path))

        monkeypatch.setattr("py_load_spl.cli.get_settings", lambda: test_settings)
        monkeypatch.setattr(
            "py_load_spl.acquisition.get_settings", lambda: test_settings
        )

        # 3. Run the 'init' command first to set up the schema
        init_result = runner.invoke(app, ["init"])
        assert init_result.exit_code == 0

        # 4. Mock the network calls
        with requests_mock.Mocker() as m:
            m.get(
                str(test_settings.fda_source_url),
                text=HTML_FIXTURE.format(checksum=mock_checksum).replace(
                    "09022025", "09092025"
                ),
            )
            m.get(
                "https://example.com/dm_spl_daily_update_09092025.zip",
                content=mock_content,
                headers={"Content-Length": str(len(mock_content))},
            )

            # 5. Run the delta-load command
            delta_result = runner.invoke(app, ["delta-load"])

        # 6. Assertions
        assert delta_result.exit_code == 0, (
            f"CLI command failed with output:\n{delta_result.stdout}"
        )
        assert "Delta load process finished successfully" in delta_result.stdout

        # 7. Verify data in the database directly
        conn = psycopg2.connect(
            dbname=postgres.dbname,
            user=postgres.username,
            password=postgres.password,
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
        )
        with conn.cursor() as cur:
            # Check that the archive was recorded
            cur.execute(
                "SELECT archive_name, archive_checksum FROM etl_processed_archives"
            )
            processed_archive = cur.fetchone()
            assert processed_archive is not None
            assert processed_archive[0] == archive_name
            assert processed_archive[1] == mock_checksum

            # Check that the product was loaded
            cur.execute(
                "SELECT document_id, set_id, product_name, manufacturer_name, dosage_form FROM products"
            )
            product = cur.fetchone()
            assert product is not None
            assert str(product[0]) == "d1b64b62-050a-4895-924c-d2862d2a6a69"
            assert str(product[1]) == "a2c3b6f0-a38f-4b48-96eb-3b2b403816a4"
            assert product[2] == "Test Drug"
            assert product[3] == "Test Corp"
            assert product[4] == "TABLET"
        conn.close()
