from datetime import date
from uuid import uuid4

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


from py_load_spl.config import PostgresSettings


@pytest.fixture(scope="module")
def postgres_loader() -> PostgresLoader:
    """
    Spins up a PostgreSQL container and yields a PostgresLoader instance
    configured to connect to it.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        db_settings = PostgresSettings(
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


def test_get_processed_archives(postgres_loader: PostgresLoader):
    """
    Tests that the loader can correctly retrieve the set of processed archive names.
    """
    # 1. Arrange
    postgres_loader.initialize_schema()
    settings = postgres_loader.settings
    conn = psycopg2.connect(
        dbname=settings.name,
        user=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
    )
    processed_files = {"file1.zip", "file2.zip", "file3.zip"}
    with conn.cursor() as cur:
        for filename in processed_files:
            cur.execute(
                "INSERT INTO etl_processed_archives (archive_name, archive_checksum, processed_timestamp) VALUES (%s, %s, NOW())",
                (filename, "dummy_checksum"),
            )
    conn.commit()
    conn.close()

    # 2. Act
    result = postgres_loader.get_processed_archives()

    # 3. Assert
    assert result == processed_files


def test_merge_from_staging_delta_load(postgres_loader: PostgresLoader):
    """
    Tests that the delta-load merge logic correctly performs UPSERTs
    and replaces child table data.
    """
    # 1. Arrange: Initial State
    postgres_loader.initialize_schema()
    settings = postgres_loader.settings
    conn = psycopg2.connect(
        dbname=settings.name,
        user=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
    )

    # ID for the document that will be updated
    updated_doc_id = uuid4()
    # ID for a document that will be untouched by the delta load
    untouched_doc_id = uuid4()
    # ID for a brand new document that will be inserted
    new_doc_id = uuid4()

    with conn.cursor() as cur:
        # Insert the 'untouched' and 'to-be-updated' documents into the production tables
        cur.execute(
            """
            INSERT INTO spl_raw_documents (document_id, set_id, version_number, effective_time, raw_data)
            VALUES (%s, %s, 1, %s, '{}'), (%s, %s, 1, %s, '{}');
            """,
            (
                str(untouched_doc_id),
                str(uuid4()),
                date(2024, 1, 1),
                str(updated_doc_id),
                str(uuid4()),
                date(2024, 1, 1),
            ),
        )
        cur.execute(
            """
            INSERT INTO products (document_id, set_id, version_number, effective_time, product_name)
            VALUES (%s, %s, 1, %s, 'Untouched Product'), (%s, %s, 1, %s, 'Original Product Name');
            """,
            (
                str(untouched_doc_id),
                str(uuid4()),
                date(2024, 1, 1),
                str(updated_doc_id),
                str(uuid4()),
                date(2024, 1, 1),
            ),
        )
        # Insert a child record for the document that will be updated
        cur.execute(
            """
            INSERT INTO ingredients (document_id, ingredient_name, is_active_ingredient)
                VALUES (%s, 'Original Ingredient', true), (%s, 'Untouched Ingredient', false);
            """,
            (str(updated_doc_id), str(untouched_doc_id)),
        )
    conn.commit()

    # 2. Arrange: Staging Data
    with conn.cursor() as cur:
        # Stage the 'updated' document with new values (version 2)
        cur.execute(
            """
            INSERT INTO products_staging (document_id, set_id, version_number, effective_time, product_name)
            VALUES (%s, %s, 2, %s, 'Updated Product Name');
            """,
            (str(updated_doc_id), str(uuid4()), date(2024, 2, 1)),
        )
        # Stage a new child record for the updated document
        cur.execute(
            """
            INSERT INTO ingredients_staging (document_id, ingredient_name, is_active_ingredient)
            VALUES (%s, 'Updated Ingredient', true);
            """,
            (str(updated_doc_id),),
        )
        # Stage the 'new' document
        cur.execute(
            """
            INSERT INTO products_staging (document_id, set_id, version_number, effective_time, product_name)
            VALUES (%s, %s, 1, %s, 'New Product');
            """,
            (str(new_doc_id), str(uuid4()), date(2024, 3, 1)),
        )
    conn.commit()

    # 3. Act
    postgres_loader.merge_from_staging(mode="delta-load")

    # 4. Assert
    with conn.cursor() as cur:
        # Check that the total number of products is now 3
        cur.execute("SELECT count(*) FROM products")
        assert cur.fetchone()[0] == 3

        # Check that the 'untouched' product is still version 1
        cur.execute(
            "SELECT version_number FROM products WHERE document_id = %s",
            (str(untouched_doc_id),),
        )
        assert cur.fetchone()[0] == 1

        # Check that the 'updated' product is now version 2
        cur.execute(
            "SELECT version_number, product_name FROM products WHERE document_id = %s",
            (str(updated_doc_id),),
        )
        version, name = cur.fetchone()
        assert version == 2
        assert name == "Updated Product Name"

        # Check that the 'new' product exists
        cur.execute(
            "SELECT product_name FROM products WHERE document_id = %s",
            (str(new_doc_id),),
        )
        assert cur.fetchone()[0] == "New Product"

        # Check that the child records for the updated product were replaced
        cur.execute(
            "SELECT ingredient_name FROM ingredients WHERE document_id = %s",
            (str(updated_doc_id),),
        )
        ingredients = [row[0] for row in cur.fetchall()]
        assert ingredients == ["Updated Ingredient"]

        # Check that the child records for the untouched product are still there
        cur.execute(
            "SELECT count(*) FROM ingredients WHERE document_id = %s",
            (str(untouched_doc_id),),
        )
        assert cur.fetchone()[0] == 1

        # Check that staging tables are empty
        cur.execute("SELECT count(*) FROM products_staging")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM ingredients_staging")
        assert cur.fetchone()[0] == 0
    conn.close()
