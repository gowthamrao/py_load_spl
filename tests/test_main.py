import hashlib
import zipfile
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings, PostgresSettings, Settings
from py_load_spl.main import run_delta_load, run_full_load

SAMPLE_XML_CONTENT = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250909" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Main Test Drug</name>
        <formCode code="C42916" displayName="TABLET" />
      </manufacturedProduct>
      <manufacturer>
        <name>Main Test Corp</name>
      </manufacturer>
    </manufacturedProduct>
  </subject>
</document>
"""


@pytest.fixture
def source_xml_dir(tmp_path: Path) -> Path:
    """Creates a directory with a sample XML file."""
    source_dir = tmp_path / "source_xmls"
    source_dir.mkdir()
    (source_dir / "test.xml").write_text(SAMPLE_XML_CONTENT)
    return source_dir


@pytest.fixture
def db_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[PostgresSettings, None, None]:
    """Spins up a PostgreSQL container and returns the connection settings."""
    container = PostgresContainer("postgres:16-alpine")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres:
        settings = PostgresSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
            adapter="postgresql",
        )
        # Initialize the schema
        from py_load_spl.db.postgres import PostgresLoader

        loader = PostgresLoader(settings)
        loader.initialize_schema()

        yield settings


@pytest.mark.integration
def test_run_full_load_integration(
    db_settings: DatabaseSettings, source_xml_dir: Path
) -> None:
    """
    Tests the run_full_load function directly against a test database.
    """
    settings = Settings(db=db_settings)

    # Run the full load
    run_full_load(settings=settings, source=source_xml_dir)

    # Verify data in the database
    conn_kwargs = db_settings.model_dump()
    conn_kwargs["dbname"] = conn_kwargs.pop("name")
    conn_kwargs.pop("adapter")
    conn_kwargs.pop("optimize_full_load", None)  # Optional field
    conn = psycopg2.connect(**conn_kwargs)
    with conn.cursor() as cur:
        cur.execute("SELECT product_name, manufacturer_name FROM products")
        product = cur.fetchone()
        assert product is not None
        assert product[0] == "Main Test Drug"
        assert product[1] == "Main Test Corp"
    conn.close()


# A simplified HTML fixture mimicking the structure of the DailyMed page
HTML_FIXTURE = """
<html><body><ul class="download">
    <li>dm_spl_daily_update_{date}.zip [ <a href="https://example.com/dm_spl_daily_update_{date}.zip">HTTPS</a> ]
        <ul><li>MD5 checksum: {checksum}</li></ul>
    </li>
</ul></body></html>
"""


@pytest.mark.integration
@patch("py_load_spl.acquisition.get_archive_list")
def test_run_delta_load_integration(
    mock_get_archive_list: MagicMock, db_settings: DatabaseSettings, tmp_path: Path
) -> None:
    """
    Tests the run_delta_load function directly, mocking the download part.
    """
    # 1. Prepare mock archive
    archive_name = "dm_spl_daily_update_09102025.zip"
    zip_file_path = tmp_path / archive_name
    with zipfile.ZipFile(zip_file_path, "w") as zf:
        zf.writestr(
            "delta_test.xml", SAMPLE_XML_CONTENT.replace("Main Test Drug", "Delta Drug")
        )

    mock_content = zip_file_path.read_bytes()
    mock_checksum = hashlib.md5(mock_content).hexdigest()

    # 2. Configure settings
    settings = Settings(db=db_settings, download_path=str(tmp_path))

    # 3. Mock the functions that perform network calls
    from py_load_spl.models import Archive

    mock_get_archive_list.return_value = [
        Archive(
            name=archive_name,
            url=f"https://example.com/{archive_name}",
            checksum=mock_checksum,
        )
    ]

    # We also need to patch download_archive to use our local file instead of downloading
    with patch("py_load_spl.acquisition.download_archive", return_value=zip_file_path):
        # 4. Run the delta load
        run_delta_load(settings=settings)

    # 5. Verify data in the database
    conn_kwargs = db_settings.model_dump()
    conn_kwargs["dbname"] = conn_kwargs.pop("name")
    conn_kwargs.pop("adapter")
    conn_kwargs.pop("optimize_full_load", None)
    conn = psycopg2.connect(**conn_kwargs)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT product_name FROM products WHERE product_name = 'Delta Drug'"
        )
        product = cur.fetchone()
        assert product is not None

        cur.execute(
            "SELECT COUNT(*) FROM etl_processed_archives WHERE archive_name = %s",
            (archive_name,),
        )
        count = cur.fetchone()[0]
        assert count == 1
    conn.close()


@pytest.mark.integration
def test_run_full_load_no_xml_files(
    db_settings: DatabaseSettings, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """
    Tests that run_full_load handles an empty source directory gracefully.
    """
    settings = Settings(db=db_settings)
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    with caplog.at_level("WARNING"):
        run_full_load(settings=settings, source=empty_dir)

    assert "No XML files found in the source. Aborting." in caplog.text


@patch("py_load_spl.main.get_db_loader")
def test_run_full_load_data_integrity_error(
    mock_get_loader: MagicMock, source_xml_dir: Path
) -> None:
    """
    Tests that a RuntimeError is raised if the number of transformed and
    loaded records do not match.
    """
    mock_loader = MagicMock()
    # Simulate loading more records than were transformed
    mock_loader.bulk_load_to_staging.return_value = 100
    mock_get_loader.return_value = mock_loader

    settings = Settings(
        db=PostgresSettings(adapter="postgresql"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        run_full_load(settings=settings, source=source_xml_dir)

    assert "Data integrity check failed!" in str(excinfo.value)
    assert "Transformed records (2)" in str(excinfo.value)
    assert "loaded records (100)" in str(excinfo.value)


@patch("py_load_spl.main.get_db_loader")
def test_run_full_load_main_exception(
    mock_get_loader: MagicMock, source_xml_dir: Path
) -> None:
    """Tests that the main exception handler in run_full_load works correctly."""
    mock_loader = MagicMock()
    mock_loader.bulk_load_to_staging.return_value = 2  # Pass integrity check
    mock_loader.merge_from_staging.side_effect = ValueError("DB merge failed")
    mock_get_loader.return_value = mock_loader
    mock_loader.start_run.return_value = 123  # Mock run_id

    settings = Settings(
        db=PostgresSettings(adapter="postgresql"),
    )

    with pytest.raises(ValueError, match="DB merge failed"):
        run_full_load(settings=settings, source=source_xml_dir)

    # Verify that the failure was logged in the history table
    mock_loader.end_run.assert_called_once_with(123, "FAILED", 0, "DB merge failed")


@patch("py_load_spl.main.get_db_loader")
@patch("py_load_spl.main.download_spl_archives")
def test_run_delta_load_no_new_archives(
    mock_download: MagicMock,
    mock_get_loader: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that run_delta_load exits gracefully when no new archives are found.
    """
    mock_download.return_value = []  # No new archives
    mock_loader = MagicMock()
    mock_loader.start_run.return_value = 456
    mock_get_loader.return_value = mock_loader
    settings = Settings(db=PostgresSettings(adapter="postgresql"))

    with caplog.at_level("INFO"):
        run_delta_load(settings)

    assert "No new archives found. Database is up-to-date." in caplog.text
    mock_loader.end_run.assert_called_once_with(456, "SUCCESS", 0, None)
