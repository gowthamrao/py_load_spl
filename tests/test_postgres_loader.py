import pytest
import psycopg2
import os

from py_load_spl.config import DatabaseSettings, get_settings
from py_load_spl.db.postgres import PostgresLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def db_settings() -> DatabaseSettings:
    """Fixture to provide database settings, ensuring we use test-specific settings."""
    return get_settings().db


def test_initialize_schema_locally(db_settings: DatabaseSettings):
    """
    FRD N003.4: Test the full schema initialization using a local database instance.
    This test assumes a PostgreSQL server is running and accessible.
    """
    test_db_name = "test_spl_loader"

    # Prepare connection arguments for psycopg2
    base_conn_args = db_settings.model_dump()
    base_conn_args.pop("adapter")  # psycopg2 doesn't know this argument
    base_conn_args["dbname"] = base_conn_args.pop("name")

    # Connect to the default 'postgres' db to create our test database
    default_conn_args = base_conn_args.copy()
    default_conn_args["dbname"] = "postgres"

    try:
        conn = psycopg2.connect(**default_conn_args)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {test_db_name};")
            cur.execute(f"CREATE DATABASE {test_db_name};")
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.fail(f"Could not connect to default 'postgres' database to set up test DB. Is PostgreSQL running? Error: {e}")

    # Now connect to the new test database and initialize the schema
    db_settings_for_loader = db_settings.copy(update={"name": test_db_name})
    loader = PostgresLoader(db_settings_for_loader)
    try:
        loader.initialize_schema()
    except Exception as e:
        pytest.fail(f"Schema initialization failed on the new test database. Error: {e}")


    # Verify that the tables were created
    verification_conn_args = base_conn_args.copy()
    verification_conn_args["dbname"] = test_db_name
    verification_conn = psycopg2.connect(**verification_conn_args)
    with verification_conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
            """
        )
        tables = [row[0] for row in cur.fetchall()]
    verification_conn.close()

    expected_tables = [
        "etl_load_history",
        "etl_processed_archives",
        "ingredients",
        "ingredients_staging",
        "marketing_status",
        "marketing_status_staging",
        "packaging",
        "packaging_staging",
        "product_ndcs",
        "product_ndcs_staging",
        "products",
        "products_staging",
        "spl_raw_documents",
        "spl_raw_documents_staging",
    ]

    assert sorted(tables) == sorted(expected_tables)
    print(f"Successfully verified creation of {len(tables)} tables in database '{test_db_name}'.")
