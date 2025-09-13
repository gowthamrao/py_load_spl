import os
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import boto3
import psycopg2
import pytest
import redshift_connector
from moto import mock_aws
from mypy_boto3_s3.client import S3Client
from pytest_mock import MockerFixture
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import RedshiftSettings, S3Settings
from py_load_spl.db.redshift import RedshiftLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def patch_redshift_connector(
    mocker: MockerFixture, postgres_container: PostgresContainer
) -> None:
    """
    Patches redshift_connector.connect to use psycopg2.connect instead,
    allowing tests to run against a standard Postgres container. This is a
    pragmatic workaround for the fact that we can't easily spin up a real
    Redshift instance for testing.
    """

    def mock_connect(*args: Any, **kwargs: Any) -> psycopg2.extensions.connection:
        # The RedshiftLoader passes Redshift-specific args; we ignore them
        # and use the connection details from the Postgres container.
        return psycopg2.connect(
            host=postgres_container.get_container_host_ip(),
            port=postgres_container.get_exposed_port(5432),
            user=postgres_container.username,
            password=postgres_container.password,
            dbname=postgres_container.dbname,
        )

    mocker.patch("redshift_connector.connect", side_effect=mock_connect)


@pytest.fixture(scope="module")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="module")
def s3_bucket_name() -> str:
    return "test-spl-bucket"


@pytest.fixture(scope="module")
def s3_client(aws_credentials: None) -> Generator[S3Client, None, None]:
    with mock_aws():
        s3 = boto3.client("s3")
        yield s3


@pytest.fixture(scope="module")
def s3_bucket(s3_client: S3Client, s3_bucket_name: str) -> Generator[None, None, None]:
    """Creates a mock S3 bucket for the tests."""
    s3_client.create_bucket(Bucket=s3_bucket_name)
    yield


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Starts a PostgreSQL container to act as a Redshift stand-in."""
    container = PostgresContainer("postgres:13")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres:
        yield postgres


@pytest.fixture(scope="module")
def redshift_settings(
    postgres_container: PostgresContainer, s3_bucket_name: str
) -> RedshiftSettings:
    """Provides RedshiftSettings configured for the test environment."""
    return RedshiftSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        name=postgres_container.dbname,
        iam_role_arn="arn:aws:iam::123456789012:role/test-role",
    )


@pytest.fixture(scope="module")
def s3_settings(s3_bucket_name: str) -> S3Settings:
    """Provides S3Settings configured for the test environment."""
    return S3Settings(bucket=s3_bucket_name, prefix="test-data")


@pytest.fixture
def redshift_loader(
    redshift_settings: RedshiftSettings, s3_settings: S3Settings, s3_bucket: None
) -> RedshiftLoader:
    """Provides an instance of RedshiftLoader connected to test resources."""
    return RedshiftLoader(redshift_settings, s3_settings)


def test_redshift_loader_pipeline(
    redshift_loader: RedshiftLoader,
    tmp_path: Path,
    s3_client: S3Client,
    s3_bucket_name: str,
    mocker: MockerFixture,
) -> None:
    """
    Tests the end-to-end process for the RedshiftLoader.
    """
    test_schema_path = Path(__file__).parent / "redshift_schema_for_test.sql"
    mocker.patch("py_load_spl.db.redshift.SQL_SCHEMA_PATH", test_schema_path)
    redshift_loader.initialize_schema()

    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    products_file = intermediate_dir / "products_staging.csv"
    doc_id = "52f41856-4220-47a7-9383-a461f8828537"
    set_id = "a1b2c3d4-e5f6-a7b8-c9d0-e1f2a3b4c5d6"
    products_file.write_text(
        f'{doc_id},{set_id},1,"2025-01-01","Test Product","Test MFG","TABLET","ORAL",false,"2025-01-01T12:00:00Z"'
    )

    mock_cursor = MagicMock()
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    redshift_loader.bulk_load_to_staging(intermediate_dir)

    s3_prefix = redshift_loader.s3_uploader.settings.prefix
    assert s3_prefix is not None
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_prefix)
    assert response["KeyCount"] == 1
    assert response["Contents"][0]["Key"] == f"{s3_prefix}/products_staging.csv"

    assert mock_cursor.execute.call_count == 1
    call_args, _ = mock_cursor.execute.call_args
    sql_command = call_args[0]
    assert "COPY products_staging" in sql_command
    assert (
        f"FROM 's3://{s3_bucket_name}/{s3_prefix}/products_staging.csv'" in sql_command
    )
    assert f"IAM_ROLE '{redshift_loader.settings.iam_role_arn}'" in sql_command
    assert "FORMAT AS CSV" in sql_command


def test_redshift_merge_logic(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests the merge logic separately to avoid complex mocking."""
    test_schema_path = Path(__file__).parent / "redshift_schema_for_test.sql"
    mocker.patch("py_load_spl.db.redshift.SQL_SCHEMA_PATH", test_schema_path)
    redshift_loader.initialize_schema()

    doc_id = "8f9b976c-396a-498c-8a9c-057088c430c5"
    set_id = "d0e0f0a0-b1c2-d3e4-f5a6-b7c8d9e0f1a2"

    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO spl_raw_documents_staging VALUES ('{doc_id}', '{set_id}', 1, '2025-01-01', '{{\"key\": \"value\"}}', 'file.xml', '2025-01-01');"
            )
            cur.execute(
                f"INSERT INTO products_staging VALUES ('{doc_id}', '{set_id}', 1, '2025-01-01', 'Prod', 'Mfg', 'Form', 'Route', false, '2025-01-01');"
            )
        conn.commit()

    redshift_loader.merge_from_staging(mode="full-load")

    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM products WHERE document_id = %s", (doc_id,)
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT COUNT(*) FROM spl_raw_documents WHERE document_id = %s",
                (doc_id,),
            )
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM products_staging;")
            assert cur.fetchone()[0] == 0
        conn.commit()


def test_redshift_merge_delta_load(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests the delta-load logic for the merge operation."""
    test_schema_path = Path(__file__).parent / "redshift_schema_for_test.sql"
    mocker.patch("py_load_spl.db.redshift.SQL_SCHEMA_PATH", test_schema_path)
    redshift_loader.initialize_schema()

    # Existing data in production table
    doc_id_v1 = str(uuid.uuid4())
    set_id = str(uuid.uuid4())
    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO products VALUES ('{doc_id_v1}', '{set_id}', 1, '2025-01-01', 'Prod', 'Mfg', 'Form', 'Route', true, '2025-01-01');"
            )
        conn.commit()

    # New data in staging table (a new version of the existing product)
    doc_id_v2 = str(uuid.uuid4())
    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO products_staging VALUES ('{doc_id_v2}', '{set_id}', 2, '2025-01-02', 'Prod v2', 'Mfg', 'Form', 'Route', false, '2025-01-02');"
            )
        conn.commit()

    # Run the delta merge
    redshift_loader.merge_from_staging(mode="delta-load")

    # Assertions
    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            # Check that the old version is gone
            cur.execute(
                "SELECT COUNT(*) FROM products WHERE document_id = %s", (doc_id_v1,)
            )
            assert cur.fetchone()[0] == 0
            # Check that the new version is there
            cur.execute(
                "SELECT COUNT(*) FROM products WHERE document_id = %s", (doc_id_v2,)
            )
            assert cur.fetchone()[0] == 1
        conn.commit()


def test_etl_tracking_methods(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests the ETL tracking methods for Redshift."""
    test_schema_path = Path(__file__).parent / "redshift_schema_for_test.sql"
    mocker.patch("py_load_spl.db.redshift.SQL_SCHEMA_PATH", test_schema_path)
    redshift_loader.initialize_schema()

    # Test start and end run
    run_id = redshift_loader.start_run("delta-load")
    assert isinstance(run_id, int)
    redshift_loader.end_run(run_id, "SUCCESS", 100, None)

    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, records_loaded FROM etl_load_history WHERE run_id = %s",
                (run_id,),
            )
            status, records = cur.fetchone()
            assert status == "SUCCESS"
            assert records == 100

    # Test processed archives
    redshift_loader.record_processed_archive("archive1.zip", "checksum1")
    processed = redshift_loader.get_processed_archives()
    assert "archive1.zip" in processed


def test_connection_error(
    mocker: MockerFixture,
    redshift_settings: RedshiftSettings,
    s3_settings: S3Settings,
) -> None:
    """Tests that a connection error is handled correctly."""
    mocker.patch(
        "redshift_connector.connect",
        side_effect=redshift_connector.Error("Connection failed"),
    )
    loader = RedshiftLoader(redshift_settings, s3_settings)
    with pytest.raises(redshift_connector.Error, match="Connection failed"):
        with loader._get_conn():
            pass  # This should fail


def test_bulk_load_copy_failure(
    redshift_loader: RedshiftLoader, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Tests failure during the Redshift COPY command."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    (intermediate_dir / "products_staging.csv").write_text("data")

    # Mock the S3 uploader to avoid actual S3 calls
    mocker.patch.object(
        redshift_loader.s3_uploader,
        "upload_directory",
        return_value="s3://test-bucket/prefix",
    )

    # Mock the cursor to raise an error on execute
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = redshift_connector.Error("COPY failed")

    # Create a mock connection object that has a rollback method
    mock_conn_obj = MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(__enter__=MagicMock(return_value=mock_cursor))
        ),
        rollback=MagicMock(),
    )
    # Manually set the .conn attribute on the loader instance so the except block can find it
    redshift_loader.conn = mock_conn_obj

    # Also mock the context manager to return our mock connection
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn_obj)),
    )

    with pytest.raises(redshift_connector.Error):
        redshift_loader.bulk_load_to_staging(intermediate_dir)

    # Verify that rollback was called on the connection object
    mock_conn_obj.rollback.assert_called_once()


def test_post_load_cleanup(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests the post-load cleanup VACUUM/ANALYZE commands."""
    mock_cursor = MagicMock()
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    redshift_loader.post_load_cleanup("full-load")

    # Check that VACUUM and ANALYZE were called
    assert mock_cursor.execute.call_count == 2
    assert "VACUUM;" in str(mock_cursor.execute.call_args_list)
    assert "ANALYZE;" in str(mock_cursor.execute.call_args_list)


def test_parquet_load_path(
    redshift_loader: RedshiftLoader, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Tests the code path for loading a Parquet file."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    (intermediate_dir / "products_staging.parquet").write_text("dummy_parquet_data")

    mocker.patch.object(
        redshift_loader.s3_uploader,
        "upload_directory",
        return_value="s3://test-bucket/prefix",
    )
    mock_cursor = MagicMock()
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    redshift_loader.bulk_load_to_staging(intermediate_dir)

    assert mock_cursor.execute.call_count == 1
    sql_command = mock_cursor.execute.call_args[0][0]
    assert "FORMAT AS PARQUET" in sql_command


def test_bulk_load_no_files(
    redshift_loader: RedshiftLoader, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Tests that bulk loading from an empty directory does nothing."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()

    mock_cursor = MagicMock()
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    rows_loaded = redshift_loader.bulk_load_to_staging(intermediate_dir)

    assert rows_loaded == 0
    mock_cursor.execute.assert_not_called()


def test_initialize_schema_not_found(
    redshift_loader: RedshiftLoader, mocker: MockerFixture, tmp_path: Path
) -> None:
    """Tests that a FileNotFoundError is raised if the schema DDL is missing."""
    non_existent_path = tmp_path / "non_existent_schema.sql"
    mocker.patch("py_load_spl.db.redshift.SQL_SCHEMA_PATH", non_existent_path)

    with pytest.raises(FileNotFoundError):
        redshift_loader.initialize_schema()


def test_start_run_no_run_id(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests that a RuntimeError is raised if the run_id cannot be fetched."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None  # Simulate not finding the new run_id
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    with pytest.raises(RuntimeError, match="Could not retrieve run_id"):
        redshift_loader.start_run("full-load")


def test_get_processed_archives_failure(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests that an empty set is returned if fetching archives fails."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = redshift_connector.Error("Query failed")
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=mocker.MagicMock(
            __enter__=mocker.MagicMock(
                return_value=mocker.MagicMock(
                    cursor=mocker.MagicMock(
                        return_value=mocker.MagicMock(
                            __enter__=mocker.MagicMock(return_value=mock_cursor)
                        )
                    )
                )
            )
        ),
    )

    result = redshift_loader.get_processed_archives()
    assert result == set()


def test_initialize_schema_failure(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests that a failure during schema initialization is handled."""
    mocker.patch("pathlib.Path.read_text", return_value="CREATE TABLE test;")
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = redshift_connector.Error("DDL failed")
    mock_conn = MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(__enter__=MagicMock(return_value=mock_cursor))
        ),
        rollback=MagicMock(),
    )
    redshift_loader.conn = mock_conn
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn)),
    )

    with pytest.raises(redshift_connector.Error):
        redshift_loader.initialize_schema()

    mock_conn.rollback.assert_called_once()


@pytest.mark.parametrize(
    "method_to_test, method_args, failing_sql_command",
    [
        ("merge_from_staging", {"mode": "full-load"}, "TRUNCATE TABLE"),
        ("start_run", {"mode": "delta-load"}, "INSERT INTO etl_load_history"),
        (
            "end_run",
            {"run_id": 1, "status": "SUCCESS", "records_loaded": 0, "error_log": None},
            "UPDATE etl_load_history",
        ),
        (
            "record_processed_archive",
            {"archive_name": "a", "checksum": "b"},
            "DELETE FROM etl_processed_archives",
        ),
    ],
)
def test_generic_redshift_failure_handling(
    redshift_loader: RedshiftLoader,
    mocker: MockerFixture,
    method_to_test: str,
    method_args: dict[str, Any],
    failing_sql_command: str,
) -> None:
    """
    A generic test to verify that rollback is called on database errors
    for various methods in the RedshiftLoader.
    """
    mock_cursor = MagicMock()

    def execute_side_effect(sql: str, *args: Any) -> None:
        if failing_sql_command in sql:
            raise redshift_connector.Error("SQL command failed")
        if "MAX(run_id)" in sql:
            mock_cursor.fetchone.return_value = (1,)

    mock_cursor.execute.side_effect = execute_side_effect

    mock_conn = MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(__enter__=MagicMock(return_value=mock_cursor))
        ),
        rollback=MagicMock(),
    )
    redshift_loader.conn = mock_conn
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn)),
    )

    method = getattr(redshift_loader, method_to_test)

    with pytest.raises(redshift_connector.Error, match="SQL command failed"):
        method(**method_args)

    mock_conn.rollback.assert_called_once()


def test_post_load_cleanup_failure(
    redshift_loader: RedshiftLoader, mocker: MockerFixture
) -> None:
    """Tests the failure path for post_load_cleanup, which uses autocommit."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = redshift_connector.Error("VACUUM failed")
    mock_conn = MagicMock(
        cursor=MagicMock(
            return_value=MagicMock(__enter__=MagicMock(return_value=mock_cursor))
        ),
        rollback=MagicMock(),  # Include a rollback mock to ensure it's NOT called
    )
    mocker.patch.object(
        redshift_loader,
        "_get_conn",
        return_value=MagicMock(__enter__=MagicMock(return_value=mock_conn)),
    )

    with pytest.raises(redshift_connector.Error, match="VACUUM failed"):
        redshift_loader.post_load_cleanup("full-load")

    mock_conn.rollback.assert_not_called()
