import unittest
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from py_load_spl.config import DatabricksSettings
from py_load_spl.db.databricks import DatabricksLoader


@pytest.fixture
def databricks_settings() -> DatabricksSettings:
    """Provides a sample DatabricksSettings for testing."""
    return DatabricksSettings(
        adapter="databricks",
        server_hostname="test.databricks.net",
        http_path="/sql/1.0/warehouses/test",
        token="test_token",
        s3_staging_path="s3://test-bucket/staging",
    )


@patch("py_load_spl.db.databricks.S3Uploader")
def test_initialization(
    mock_s3_uploader: MagicMock, databricks_settings: DatabricksSettings
) -> None:
    """Tests that the loader initializes correctly and parses the S3 path."""
    loader = DatabricksLoader(databricks_settings)
    assert loader is not None
    mock_s3_uploader.assert_called_once()
    # Check that the S3Uploader was called with settings derived from the path
    args, kwargs = mock_s3_uploader.call_args
    s3_settings = args[0]
    assert s3_settings.bucket == "test-bucket"
    assert s3_settings.prefix == "staging"


@patch("py_load_spl.db.databricks.sql.connect")
def test_initialize_schema(
    mock_connect: MagicMock, databricks_settings: DatabricksSettings
) -> None:
    """Tests that initialize_schema executes all statements from the DDL file."""
    loader = DatabricksLoader(databricks_settings)
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock the content of the schema file
    schema_content = "CREATE TABLE table1 (id INT); CREATE TABLE table2 (id INT);"
    with patch("builtins.open", unittest.mock.mock_open(read_data=schema_content)):
        loader.initialize_schema()

    # Should be called once for each statement
    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_has_calls(
        [call("CREATE TABLE table1 (id INT)"), call("CREATE TABLE table2 (id INT)")],
        any_order=False,
    )


@patch("py_load_spl.db.databricks.sql.connect")
@patch("py_load_spl.db.databricks.S3Uploader.upload_directory")
def test_bulk_load_to_staging_with_chunked_files(
    mock_upload: MagicMock,
    mock_connect: MagicMock,
    databricks_settings: DatabricksSettings,
    tmp_path: Path,
) -> None:
    """Tests that bulk_load_to_staging handles multiple chunked files correctly."""
    loader = DatabricksLoader(databricks_settings)
    mock_upload.return_value = "s3://test-bucket/staging"
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Create some dummy intermediate files
    (tmp_path / "products_0.csv").touch()
    (tmp_path / "products_1.csv").touch()
    (tmp_path / "ingredients_0.csv").touch()

    loader.bulk_load_to_staging(tmp_path)

    # Should be 3 COPY INTO calls
    assert mock_cursor.execute.call_count == 3
    sql_calls = [args[0] for args, kwargs in mock_cursor.execute.call_args_list]

    # Check that the generated SQL is correct
    assert any("COPY INTO products_staging" in s for s in sql_calls)
    assert any("FROM 's3://test-bucket/staging/products_0.csv'" in s for s in sql_calls)
    assert any("FROM 's3://test-bucket/staging/products_1.csv'" in s for s in sql_calls)
    assert any("COPY INTO ingredients_staging" in s for s in sql_calls)
    assert any(
        "FROM 's3://test-bucket/staging/ingredients_0.csv'" in s for s in sql_calls
    )


@patch("py_load_spl.db.databricks.sql.connect")
def test_merge_from_staging_full_load(
    mock_connect: MagicMock, databricks_settings: DatabricksSettings
) -> None:
    """Tests the merge_from_staging method in full-load mode."""
    loader = DatabricksLoader(databricks_settings)
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    loader.merge_from_staging("full-load")
    # 6 tables to truncate, 6 to insert from
    assert mock_cursor.execute.call_count == 12
    sql_calls = [args[0] for args, kwargs in mock_cursor.execute.call_args_list]
    assert "TRUNCATE TABLE products" in sql_calls
    assert "INSERT INTO products SELECT * FROM products_staging" in sql_calls


@patch("py_load_spl.db.databricks.sql.connect")
def test_merge_from_staging_delta_load(
    mock_connect: MagicMock, databricks_settings: DatabricksSettings
) -> None:
    """Tests the merge_from_staging method in delta-load mode."""
    loader = DatabricksLoader(databricks_settings)
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    loader.merge_from_staging("delta-load")
    # 6 tables to merge
    assert mock_cursor.execute.call_count == 6
    sql_calls = [args[0] for args, kwargs in mock_cursor.execute.call_args_list]
    assert any("MERGE INTO products AS target" in s for s in sql_calls)
    assert any("ON target.document_id = source.document_id" in s for s in sql_calls)


@patch("py_load_spl.db.databricks.sql.connect")
def test_etl_tracking(
    mock_connect: MagicMock, databricks_settings: DatabricksSettings
) -> None:
    """Tests the ETL tracking methods."""
    loader = DatabricksLoader(databricks_settings)
    mock_cursor = MagicMock()
    mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    # Test start_run
    mock_cursor.fetchone.return_value = (123,)
    run_id = loader.start_run("delta-load")
    assert run_id == 123
    mock_cursor.execute.assert_has_calls(
        [
            call(
                "INSERT INTO etl_load_history (start_time, status, mode) VALUES (current_timestamp(), 'RUNNING', %s)",
                ("delta-load",),
            ),
            call("SELECT MAX(run_id) FROM etl_load_history"),
        ]
    )

    # Test end_run
    loader.end_run(123, "SUCCESS", 500, None)
    end_run_sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE etl_load_history" in end_run_sql
    assert "SET end_time = current_timestamp()" in end_run_sql

    # Test record_processed_archive
    loader.record_processed_archive("test_archive.zip", "checksum123")
    record_sql = mock_cursor.execute.call_args[0][0]
    assert "INSERT INTO etl_processed_archives" in record_sql
    assert "VALUES (%s, %s, current_timestamp())" in record_sql
