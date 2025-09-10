from pathlib import Path
from unittest.mock import MagicMock

import psycopg2
import pytest

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader


from py_load_spl.config import PostgresSettings


@pytest.fixture
def db_settings(tmp_path: Path) -> PostgresSettings:
    """Provides a standard DatabaseSettings instance for tests."""
    return PostgresSettings(
        host="localhost",
        port=5432,
        user="test",
        password="test",
        name="testdb",
    )


def test_get_conn_failure(db_settings: DatabaseSettings, monkeypatch):
    """Tests that a connection error is properly raised."""
    monkeypatch.setattr(
        psycopg2,
        "connect",
        MagicMock(side_effect=psycopg2.OperationalError("Connection failed")),
    )
    loader = PostgresLoader(db_settings)
    with pytest.raises(psycopg2.OperationalError):
        # initialize_schema is a simple way to trigger _get_conn
        loader.initialize_schema()


def test_initialize_schema_file_not_found(db_settings: DatabaseSettings, monkeypatch):
    """Tests the case where the schema.sql file is missing."""
    # Mock the path to a non-existent file
    monkeypatch.setattr(
        "py_load_spl.db.postgres.SQL_SCHEMA_PATH", Path("/non/existent/path.sql")
    )
    loader = PostgresLoader(db_settings)
    with pytest.raises(FileNotFoundError):
        loader.initialize_schema()


def test_get_processed_archives_db_error(db_settings: DatabaseSettings, monkeypatch):
    """Tests that get_processed_archives handles a database error gracefully."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.Error("DB error")

    monkeypatch.setattr(psycopg2, "connect", MagicMock(return_value=mock_conn))

    loader = PostgresLoader(db_settings)
    # It should return an empty set and not raise an exception
    assert loader.get_processed_archives() == set()


def test_record_processed_archive_db_error(db_settings: DatabaseSettings, monkeypatch):
    """Tests that record_processed_archive raises an exception on DB error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.Error("DB error")

    monkeypatch.setattr(psycopg2, "connect", MagicMock(return_value=mock_conn))

    loader = PostgresLoader(db_settings)
    with pytest.raises(psycopg2.Error):
        loader.record_processed_archive("archive.zip", "checksum")


def test_bulk_load_staging_file_missing(
    db_settings: DatabaseSettings, tmp_path: Path, monkeypatch
):
    """Tests that bulk loading skips a missing file without error."""
    # This test only needs to ensure the connection is mocked, as the logic for file checking
    # happens before any cursors are created or used.
    monkeypatch.setattr(psycopg2, "connect", MagicMock())

    loader = PostgresLoader(db_settings)
    # We expect this to run without raising an exception, and it should log a warning (which we can't easily test here)
    # The main point is that it doesn't crash the pipeline.
    try:
        loader.bulk_load_to_staging(tmp_path)
    except Exception as e:
        pytest.fail(
            f"bulk_load_to_staging should not have raised an exception for missing files, but raised {e}"
        )
