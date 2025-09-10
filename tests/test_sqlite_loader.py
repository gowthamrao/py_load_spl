import sqlite3
from pathlib import Path

import pytest

from py_load_spl.config import Settings
from py_load_spl.db.sqlite import SqliteLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


from py_load_spl.config import SqliteSettings


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    """Creates a Settings object configured for a temporary SQLite database."""
    db_path = tmp_path / "test_spl.db"
    return Settings(
        db=SqliteSettings(name=str(db_path)),
        intermediate_format="csv",
    )


@pytest.fixture
def sqlite_loader(test_settings: Settings) -> SqliteLoader:
    """Yields a SqliteLoader instance for testing."""
    return SqliteLoader(test_settings.db)


def test_initialize_schema(sqlite_loader: SqliteLoader):
    """Tests that the SQLite schema is created correctly."""
    # Act
    sqlite_loader.initialize_schema()

    # Assert
    with sqlite3.connect(sqlite_loader.db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = {row[0] for row in cur.fetchall()}

    expected_tables = {
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
    }
    assert tables == expected_tables


def _create_dummy_csv_files(tmp_path: Path) -> Path:
    """Helper function to create dummy CSV files for testing."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()

    # Create products.csv
    (intermediate_dir / "products.csv").write_text(
        "doc1,set1,1,2024-01-01,Product A,Pfizer,Tablet,Oral,1,2024-01-01T12:00:00\n"
        "doc2,set1,2,2024-02-01,Product A V2,Pfizer,Tablet,Oral,0,2024-02-01T12:00:00\n"
    )
    # Create ingredients.csv
    (intermediate_dir / "ingredients.csv").write_text(
        "doc1,A,UNII-A,10,100,mL,1\ndoc2,B,UNII-B,20,100,mL,1\n"
    )
    # Create spl_raw_documents.csv
    (intermediate_dir / "spl_raw_documents.csv").write_text(
        'doc1,set1,1,2024-01-01,{"key": "value1"},file1.zip,2024-01-01T12:00:00\n'
        'doc2,set1,2,2024-02-01,{"key": "value2"},file2.zip,2024-02-01T12:00:00\n'
    )
    # Create empty files for other tables to ensure they are handled gracefully
    (intermediate_dir / "product_ndcs.csv").touch()
    (intermediate_dir / "packaging.csv").touch()
    (intermediate_dir / "marketing_status.csv").touch()

    return intermediate_dir


def test_full_load_and_etl_tracking(sqlite_loader: SqliteLoader, tmp_path: Path):
    """
    Tests a full, end-to-end load process and the ETL tracking methods.
    """
    # 1. Arrange
    intermediate_dir = _create_dummy_csv_files(tmp_path)
    sqlite_loader.initialize_schema()

    # 2. Act
    run_id = sqlite_loader.start_run(mode="full-load")
    sqlite_loader.bulk_load_to_staging(intermediate_dir)
    sqlite_loader.merge_from_staging(mode="full-load")
    sqlite_loader.record_processed_archive("file1.zip", "checksum1")
    sqlite_loader.end_run(run_id, "SUCCESS", 2, None)

    # 3. Assert
    with sqlite3.connect(sqlite_loader.db_path) as conn:
        cur = conn.cursor()
        # Verify data loaded correctly
        cur.execute("SELECT COUNT(*) FROM products;")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT product_name FROM products WHERE document_id = 'doc1';")
        assert cur.fetchone()[0] == "Product A"

        # Verify ETL history
        cur.execute(
            "SELECT status, records_loaded FROM etl_load_history WHERE run_id = ?;",
            (run_id,),
        )
        status, count = cur.fetchone()
        assert status == "SUCCESS"
        assert count == 2

        # Verify processed archives
        cur.execute(
            "SELECT COUNT(*) FROM etl_processed_archives WHERE archive_name = 'file1.zip';"
        )
        assert cur.fetchone()[0] == 1

        # Verify staging tables are empty
        cur.execute("SELECT COUNT(*) FROM products_staging;")
        assert cur.fetchone()[0] == 0


def test_delta_load_logic(sqlite_loader: SqliteLoader, tmp_path: Path):
    """
    Tests that the delta-load merge logic correctly UPSERTs and replaces data.
    """
    # 1. Arrange: Initial full load
    intermediate_dir_v1 = tmp_path / "intermediate_v1"
    intermediate_dir_v1.mkdir()
    # Version 1 of 'doc1'
    (intermediate_dir_v1 / "products.csv").write_text(
        "doc1,set1,1,2024-01-01,Product A,Pfizer,Tablet,Oral,1,2024-01-01T12:00:00\n"
    )
    (intermediate_dir_v1 / "ingredients.csv").write_text("doc1,A,UNII-A,10,100,mL,1\n")
    (intermediate_dir_v1 / "spl_raw_documents.csv").write_text(
        'doc1,set1,1,2024-01-01,{"key": "value1"},file1.zip,2024-01-01T12:00:00\n'
    )
    (intermediate_dir_v1 / "product_ndcs.csv").touch()
    (intermediate_dir_v1 / "packaging.csv").touch()
    (intermediate_dir_v1 / "marketing_status.csv").touch()

    sqlite_loader.initialize_schema()
    sqlite_loader.bulk_load_to_staging(intermediate_dir_v1)
    sqlite_loader.merge_from_staging(
        mode="full-load"
    )  # is_latest_version is now True for doc1

    # 2. Arrange: Delta load data
    intermediate_dir_v2 = tmp_path / "intermediate_v2"
    intermediate_dir_v2.mkdir()
    # Version 2 of 'doc1' and a new document 'doc2'
    (intermediate_dir_v2 / "products.csv").write_text(
        "doc1,set1,2,2024-02-01,Product A V2,Pfizer,Tablet,Oral,0,2024-02-01T12:00:00\n"
        "doc2,set2,1,2024-03-01,Product B,Moderna,Capsule,Oral,1,2024-03-01T12:00:00\n"
    )
    (intermediate_dir_v2 / "ingredients.csv").write_text(
        "doc1,B,UNII-B,20,100,mL,1\n"
    )  # Replaces ingredient for doc1
    (intermediate_dir_v2 / "spl_raw_documents.csv").write_text(
        'doc1,set1,2,2024-02-01,{"key": "value2"},file2.zip,2024-02-01T12:00:00\n'
        'doc2,set2,1,2024-03-01,{"key": "value3"},file3.zip,2024-03-01T12:00:00\n'
    )
    (intermediate_dir_v2 / "product_ndcs.csv").touch()
    (intermediate_dir_v2 / "packaging.csv").touch()
    (intermediate_dir_v2 / "marketing_status.csv").touch()

    # 3. Act
    sqlite_loader.bulk_load_to_staging(intermediate_dir_v2)
    sqlite_loader.merge_from_staging(mode="delta-load")

    # 4. Assert
    with sqlite3.connect(sqlite_loader.db_path) as conn:
        cur = conn.cursor()
        # Total products should be 2
        cur.execute("SELECT COUNT(*) FROM products;")
        assert cur.fetchone()[0] == 2

        # Check that 'doc1' was updated
        cur.execute(
            "SELECT version_number, product_name, is_latest_version FROM products WHERE document_id = 'doc1';"
        )
        version, name, is_latest = cur.fetchone()
        assert version == 2
        assert name == "Product A V2"
        assert is_latest == 1  # The update logic should have made this the latest

        # Check that the ingredients for 'doc1' were replaced
        cur.execute(
            "SELECT ingredient_name FROM ingredients WHERE document_id = 'doc1';"
        )
        assert cur.fetchone()[0] == "B"

        # Check that 'doc2' was inserted
        cur.execute("SELECT COUNT(*) FROM products WHERE document_id = 'doc2';")
        assert cur.fetchone()[0] == 1
