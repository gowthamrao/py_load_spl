import os
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws
from testcontainers.postgres import PostgresContainer
from pytest_mock import MockerFixture
from mypy_boto3_s3.client import S3Client

from py_load_spl.config import RedshiftSettings, S3Settings
from py_load_spl.db.redshift import RedshiftLoader

import psycopg2


# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def patch_redshift_connector(mocker: MockerFixture, postgres_container: PostgresContainer):
    """
    Patches redshift_connector.connect to use psycopg2.connect instead,
    allowing tests to run against a standard Postgres container. This is a
    pragmatic workaround for the fact that we can't easily spin up a real
    Redshift instance for testing.
    """

    def mock_connect(*args, **kwargs):
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
    with PostgresContainer("postgres:13") as container:
        yield container


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
    mocker.patch.object(redshift_loader, "_get_conn", return_value=mocker.MagicMock(__enter__=mocker.MagicMock(return_value=mocker.MagicMock(cursor=mocker.MagicMock(return_value=mocker.MagicMock(__enter__=mocker.MagicMock(return_value=mock_cursor)))))))

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
    assert f"FROM 's3://{s3_bucket_name}/{s3_prefix}/products_staging.csv'" in sql_command
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
            cur.execute(f"INSERT INTO spl_raw_documents_staging VALUES ('{doc_id}', '{set_id}', 1, '2025-01-01', '{{\"key\": \"value\"}}', 'file.xml', '2025-01-01');")
            cur.execute(f"INSERT INTO products_staging VALUES ('{doc_id}', '{set_id}', 1, '2025-01-01', 'Prod', 'Mfg', 'Form', 'Route', false, '2025-01-01');")
        conn.commit()

    redshift_loader.merge_from_staging(mode="full-load")

    with redshift_loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products WHERE document_id = %s", (doc_id,))
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM spl_raw_documents WHERE document_id = %s", (doc_id,))
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM products_staging;")
            assert cur.fetchone()[0] == 0
        conn.commit()
