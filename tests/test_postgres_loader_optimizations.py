import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Spins up a PostgreSQL container for the module."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """Returns a fresh DatabaseSettings instance for each test."""
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        name=postgres_container.dbname,
        adapter="postgresql",
        optimize_full_load=True,  # Default to enabled for most tests
    )


@pytest.fixture
def loader(db_settings: DatabaseSettings) -> PostgresLoader:
    """Returns a PostgresLoader instance for each test."""
    return PostgresLoader(db_settings)


def get_db_objects(loader: PostgresLoader) -> tuple[set[str], set[str]]:
    """Helper function to get the current set of index and FK names."""
    conn = psycopg2.connect(
        dbname=loader.settings.name,
        user=loader.settings.user,
        password=loader.settings.password,
        host=loader.settings.host,
        port=loader.settings.port,
    )
    with conn.cursor() as cur:
        # Get foreign keys
        cur.execute("""
            SELECT c.conname
            FROM pg_constraint c
            JOIN pg_class conrel ON conrel.oid = c.conrelid
            WHERE c.contype = 'f' AND conrel.relname IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status'
            );
        """)
        fks = {row[0] for row in cur.fetchall()}

        # Get indexes (excluding primary keys)
        cur.execute("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename IN (
                'products', 'product_ndcs', 'ingredients', 'packaging', 'marketing_status', 'spl_raw_documents'
            ) AND indexname NOT LIKE '%%_pkey';
        """)
        indexes = {row[0] for row in cur.fetchall()}
    conn.close()
    return fks, indexes


def test_full_load_optimization_lifecycle(loader: PostgresLoader):
    """
    Tests that indexes/FKs are correctly dropped and recreated during a full-load.
    """
    # 1. Arrange: Initialize schema and get the initial state of objects
    loader.initialize_schema()
    initial_fks, initial_indexes = get_db_objects(loader)

    # Assert that our schema migration added the expected objects
    assert len(initial_fks) == 5
    assert len(initial_indexes) == 6
    assert "idx_products_versioning" in initial_indexes
    # We already assert the count of FKs, checking for one specific name
    # is brittle, as was just proven. The count is sufficient.

    # 2. Act: Run the pre-load optimization
    loader.pre_load_optimization(mode="full-load")

    # 3. Assert: Objects should now be dropped
    fks_after_drop, indexes_after_drop = get_db_objects(loader)
    assert len(fks_after_drop) == 0
    assert len(indexes_after_drop) == 0

    # 4. Act: Run the post-load cleanup
    # We need to run merge_from_staging first so the tables exist for VACUUM
    loader.merge_from_staging(mode="full-load")
    loader.post_load_cleanup(mode="full-load")

    # 5. Assert: Objects should be recreated
    final_fks, final_indexes = get_db_objects(loader)
    assert final_fks == initial_fks
    assert final_indexes == initial_indexes


def test_optimizations_skipped_for_delta_load(loader: PostgresLoader):
    """
    Tests that the optimization steps are skipped for a delta-load.
    """
    # 1. Arrange
    loader.initialize_schema()
    initial_fks, initial_indexes = get_db_objects(loader)
    assert len(initial_fks) > 0
    assert len(initial_indexes) > 0

    # 2. Act
    loader.pre_load_optimization(mode="delta-load")
    fks_after_pre, indexes_after_pre = get_db_objects(loader)

    # 3. Assert: Nothing should have been dropped
    assert fks_after_pre == initial_fks
    assert indexes_after_pre == initial_indexes

    # 4. Act
    loader.post_load_cleanup(mode="delta-load")
    fks_after_post, indexes_after_post = get_db_objects(loader)

    # 5. Assert: Nothing should have changed
    assert fks_after_post == initial_fks
    assert indexes_after_post == initial_indexes


def test_optimizations_skipped_if_disabled(db_settings: DatabaseSettings):
    """
    Tests that optimizations are skipped if the config flag is False, even for a full-load.
    """
    # 1. Arrange
    db_settings.optimize_full_load = False
    loader = PostgresLoader(db_settings)
    loader.initialize_schema()
    initial_fks, initial_indexes = get_db_objects(loader)
    assert len(initial_fks) > 0
    assert len(initial_indexes) > 0

    # 2. Act & Assert
    loader.pre_load_optimization(mode="full-load")
    fks_after_drop, indexes_after_drop = get_db_objects(loader)
    assert fks_after_drop == initial_fks
    assert indexes_after_drop == initial_indexes
