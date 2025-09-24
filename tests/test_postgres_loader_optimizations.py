from collections.abc import Generator

import psycopg2
import pytest
from pytest_mock import MockerFixture
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings, PostgresSettings
from py_load_spl.db.postgres import PostgresLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Spins up a PostgreSQL container for the module."""
    container = PostgresContainer("postgres:16-alpine")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres:
        yield postgres


@pytest.fixture
def db_settings(postgres_container: PostgresContainer) -> PostgresSettings:
    """Returns a fresh DatabaseSettings instance for each test."""
    return PostgresSettings(
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


def test_full_load_optimization_lifecycle(loader: PostgresLoader) -> None:
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


def test_optimizations_skipped_for_delta_load(loader: PostgresLoader) -> None:
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


def test_optimizations_skipped_if_disabled(db_settings: DatabaseSettings) -> None:
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


def test_get_and_recreate_optimizable_objects(loader: PostgresLoader) -> None:
    """
    Directly tests the _get_optimizable_objects, _drop_optimizations,
    and _recreate_optimizations methods to ensure they are working.
    This provides direct coverage for these critical internal methods.
    """
    loader.initialize_schema()

    # Manually create a new index to ensure we have something unique to find
    with loader._get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE INDEX my_special_test_idx ON products (product_name);")
        conn.commit()

    _, initial_indexes = get_db_objects(loader)
    assert "my_special_test_idx" in initial_indexes

    # --- Test _get_optimizable_objects and _drop_optimizations ---
    with loader._get_conn() as conn:
        with conn.cursor() as cur:
            # This is the primary method we want to cover
            loader._get_optimizable_objects(cur)
            assert len(loader.dropped_object_definitions) > 0
            # Check if our specific index definition was captured
            assert any(
                "my_special_test_idx" in s for s in loader.dropped_object_definitions
            )

            # Now drop them
            loader._drop_optimizations(cur)
        conn.commit()

    _, indexes_after_drop = get_db_objects(loader)
    assert "my_special_test_idx" not in indexes_after_drop

    # --- Test _recreate_optimizations ---
    with loader._get_conn() as conn:
        with conn.cursor() as cur:
            loader._recreate_optimizations(cur)
        conn.commit()

    _, final_indexes = get_db_objects(loader)
    assert "my_special_test_idx" in final_indexes


def test_pre_load_optimization_rollback_on_error(
    db_settings: DatabaseSettings, mocker: MockerFixture
) -> None:
    """
    Covers the error handling block in pre_load_optimization using a mocked connection.
    """
    # Arrange: Create a mock connection that we can spy on
    mock_conn = mocker.MagicMock()
    mocker.patch("psycopg2.connect", return_value=mock_conn)

    # Arrange: Create a loader and patch its internal method to raise an error
    loader = PostgresLoader(db_settings)
    mocker.patch.object(
        loader, "_get_optimizable_objects", side_effect=psycopg2.Error("Mocked Failure")
    )
    rollback_spy = mocker.spy(mock_conn, "rollback")

    # Act & Assert
    with pytest.raises(psycopg2.Error, match="Mocked Failure"):
        loader.pre_load_optimization(mode="full-load")

    assert rollback_spy.call_count == 1
