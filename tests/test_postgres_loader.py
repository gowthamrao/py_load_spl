import pytest
import psycopg2
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_loader() -> PostgresLoader:
    """
    Spins up a PostgreSQL container and yields a PostgresLoader instance
    configured to connect to it.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        db_settings = DatabaseSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
            adapter="postgresql",
        )
        loader = PostgresLoader(db_settings)
        yield loader


def test_initialize_schema(postgres_loader: PostgresLoader):
    """
    FRD N003.4: Test the full schema initialization using a test container.
    """
    # 1. Arrange
    # The postgres_loader fixture already provides the loader instance.

    # 2. Act
    try:
        postgres_loader.initialize_schema()
    except Exception as e:
        pytest.fail(f"Schema initialization failed on the test container. Error: {e}")

    # 3. Assert
    # Verify that the tables were created by connecting directly
    settings = postgres_loader.settings
    conn = psycopg2.connect(
        dbname=settings.name,
        user=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
            """
        )
        tables = [row[0] for row in cur.fetchall()]
    conn.close()

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
    print(f"Successfully verified creation of {len(tables)} tables.")
