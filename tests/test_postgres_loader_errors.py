from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest
from pytest_mock import MockerFixture

from py_load_spl.config import DatabaseSettings, PostgresSettings
from py_load_spl.db.postgres import PostgresLoader


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


def test_get_conn_failure(
    db_settings: DatabaseSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
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


def test_initialize_schema_file_not_found(
    db_settings: DatabaseSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests the case where the schema.sql file is missing."""
    # Mock the path to a non-existent file
    monkeypatch.setattr(
        "py_load_spl.db.postgres.SQL_SCHEMA_PATH", Path("/non/existent/path.sql")
    )
    loader = PostgresLoader(db_settings)
    with pytest.raises(FileNotFoundError):
        loader.initialize_schema()


def test_get_processed_archives_db_error(
    db_settings: DatabaseSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that get_processed_archives handles a database error gracefully."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.Error("DB error")

    monkeypatch.setattr(psycopg2, "connect", MagicMock(return_value=mock_conn))

    loader = PostgresLoader(db_settings)
    # It should return an empty set and not raise an exception
    assert loader.get_processed_archives() == set()


def test_record_processed_archive_db_error(
    db_settings: DatabaseSettings, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    db_settings: DatabaseSettings, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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


# --- Tests for Rollback on Error ---


@pytest.fixture
def loader_with_mock_conn(
    db_settings: PostgresSettings, monkeypatch: pytest.MonkeyPatch
) -> tuple[PostgresLoader, MagicMock]:
    """
    Provides a PostgresLoader instance where the connection is already mocked.
    This is useful for testing error handling after a connection is established.
    """
    mock_conn = MagicMock()
    # This simulates an already-established connection
    monkeypatch.setattr(psycopg2, "connect", MagicMock(return_value=mock_conn))
    loader = PostgresLoader(db_settings)
    # Manually set the connection object on the loader instance
    loader.conn = mock_conn
    return loader, mock_conn


@pytest.mark.parametrize(
    "method_to_test, method_args, patch_target",
    [
        (
            "initialize_schema",
            [],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "record_processed_archive",
            ["archive.zip", "checksum"],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "bulk_load_to_staging",
            [Path("dummy_path")],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "pre_load_optimization",
            ["full-load"],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "merge_from_staging",
            ["full-load"],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "start_run",
            ["full-load"],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
        (
            "end_run",
            [1, "SUCCESS", 0, None],
            "py_load_spl.db.postgres.PostgresLoader._get_conn",
        ),
    ],
)
def test_db_methods_rollback_on_error(
    loader_with_mock_conn: tuple[PostgresLoader, MagicMock],
    mocker: MockerFixture,
    method_to_test: str,
    method_args: list[Any],
    patch_target: str,
) -> None:
    """
    Tests that various database operations correctly call rollback() on failure.
    This covers the `if self.conn: self.conn.rollback()` lines.
    """
    loader, mock_conn = loader_with_mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = psycopg2.Error("Simulated DB Error")

    # Spy on the rollback method
    rollback_spy = mocker.spy(mock_conn, "rollback")

    # Get the actual method from the loader instance
    func_to_call = getattr(loader, method_to_test)

    # We need to mock the Path.read_text for initialize_schema
    if method_to_test == "initialize_schema":
        mocker.patch("pathlib.Path.read_text", return_value="SELECT 1;")

    # We need to mock glob for bulk_load_to_staging
    if method_to_test == "bulk_load_to_staging":
        mocker.patch(
            "pathlib.Path.glob", return_value=[Path("dummy_path/products.csv")]
        )
        mocker.patch("builtins.open", mocker.mock_open(read_data="data"))
        # IMPORTANT: bulk_load uses copy_expert, not execute
        mock_cursor.copy_expert.side_effect = psycopg2.Error("Simulated DB Error")
    else:
        mock_cursor.execute.side_effect = psycopg2.Error("Simulated DB Error")

    with pytest.raises(psycopg2.Error, match="Simulated DB Error"):
        func_to_call(*method_args)

    # Assert that rollback was called exactly once
    assert rollback_spy.call_count == 1


def test_post_load_cleanup_exception_handling(
    loader_with_mock_conn: tuple[PostgresLoader, MagicMock],
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Tests that post_load_cleanup correctly logs and re-raises an exception
    that occurs during the cleanup process.
    """
    loader, mock_conn = loader_with_mock_conn
    loader.settings.optimize_full_load = True

    # Mock the internal call to _recreate_optimizations to fail
    mocker.patch.object(
        loader,
        "_recreate_optimizations",
        side_effect=psycopg2.Error("Recreation Failed"),
    )

    with pytest.raises(psycopg2.Error, match="Recreation Failed"):
        loader.post_load_cleanup(mode="full-load")

    # Check that the error was logged correctly
    assert "Post-load cleanup failed: Recreation Failed" in caplog.text
