from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from py_load_spl.config import SqliteSettings
from py_load_spl.db.sqlite import SqliteLoader

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration

# A list of all expected tables in the schema
EXPECTED_TABLES = [
    "etl_load_history",
    "etl_processed_archives",
    "spl_raw_documents",
    "products",
    "product_ndcs",
    "ingredients",
    "packaging",
    "marketing_status",
    "spl_raw_documents_staging",
    "products_staging",
    "product_ndcs_staging",
    "ingredients_staging",
    "packaging_staging",
    "marketing_status_staging",
]


@pytest.fixture
def db_settings(tmp_path: Path) -> SqliteSettings:
    """Fixture for database settings pointing to a temporary file."""
    db_file = tmp_path / "test_spl.db"
    return SqliteSettings(
        name=str(db_file),
        optimize_full_load=True,
    )


@pytest.fixture
def sqlite_loader(db_settings: SqliteSettings) -> Generator[SqliteLoader, None, None]:
    """Fixture to provide an initialized SqliteLoader instance."""
    loader = SqliteLoader(db_settings)
    loader.initialize_schema()
    yield loader
    loader.close_conn()


def test_initialize_schema(sqlite_loader: SqliteLoader) -> None:
    """Verify that all tables are created after initialization."""
    with sqlite_loader._get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cur.fetchall()}
        assert set(EXPECTED_TABLES).issubset(tables)


def test_etl_tracking_isolated(sqlite_loader: SqliteLoader) -> None:
    """Test the ETL tracking methods in isolation."""
    assert sqlite_loader.get_processed_archives() == set()
    run_id = sqlite_loader.start_run(mode="full-load")
    assert run_id > 0
    sqlite_loader.record_processed_archive("archive1.zip", "checksum1")
    assert sqlite_loader.get_processed_archives() == {"archive1.zip"}
    sqlite_loader.end_run(run_id, "SUCCESS", 100, None)
    with sqlite_loader._get_conn() as conn:
        res = conn.execute(
            "SELECT status, records_loaded FROM etl_load_history WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        assert res[0] == "SUCCESS"
        assert res[1] == 100


def test_bulk_and_full_merge(sqlite_loader: SqliteLoader, tmp_path: Path) -> None:
    """Test bulk loading to staging and a full merge to production."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    (intermediate_dir / "spl_raw_documents.csv").write_text(
        'doc1,set1,1,2025-01-01,{"key":"value"},file.zip,2025-01-01T12:00:00\n'
    )
    (intermediate_dir / "products.csv").write_text(
        "doc1,set1,1,2025-01-01,Product A,Pfizer,Tablet,Oral,1,2025-01-01T12:00:00\n"
    )
    (intermediate_dir / "ingredients.csv").write_text("doc1,Aspirin,UNII1,81,mg,mg,1\n")
    sqlite_loader.bulk_load_to_staging(intermediate_dir)
    sqlite_loader.merge_from_staging(mode="full-load")
    with sqlite_loader._get_conn() as conn:
        res = conn.execute(
            "SELECT product_name FROM products WHERE document_id = 'doc1'"
        ).fetchone()
        assert res[0] == "Product A"


from typing import Any


def _create_intermediate_files(
    tmp_path: Path, file_format: str, data: dict[str, list[tuple[Any, ...]]]
) -> None:
    """Helper to create CSV or Parquet files for testing."""
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir(exist_ok=True)

    for table_name, rows in data.items():
        if not rows:
            continue

        filepath = intermediate_dir / f"{table_name}.{file_format}"
        if file_format == "csv":
            import csv

            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                # Write rows, converting None to the expected '\\N' null marker
                writer.writerows(
                    [["\\N" if item is None else item for item in row] for row in rows]
                )
        elif file_format == "parquet":
            # Create a PyArrow table from the rows.
            # This is more complex because we need to define a schema.
            # For simplicity in testing, we assume string types for most things.
            if table_name == "products":
                schema = pa.schema(
                    [
                        pa.field("document_id", pa.string()),
                        pa.field("set_id", pa.string()),
                        pa.field("version_number", pa.int64()),
                        pa.field("effective_time", pa.string()),
                        pa.field("product_name", pa.string()),
                        pa.field("manufacturer_name", pa.string()),
                        pa.field("dosage_form", pa.string()),
                        pa.field("route_of_administration", pa.string()),
                        pa.field("is_latest_version", pa.int64()),
                        pa.field("loaded_at", pa.string()),
                    ]
                )
            elif table_name == "ingredients":
                schema = pa.schema(
                    [
                        pa.field("document_id", pa.string()),
                        pa.field("ingredient_name", pa.string()),
                        pa.field("substance_code", pa.string()),
                        pa.field("strength_numerator", pa.string()),
                        pa.field("strength_denominator", pa.string()),
                        pa.field("unit_of_measure", pa.string()),
                        pa.field("is_active_ingredient", pa.int64()),
                    ]
                )
            else:  # spl_raw_documents
                schema = pa.schema(
                    [
                        pa.field("document_id", pa.string()),
                        pa.field("set_id", pa.string()),
                        pa.field("version_number", pa.int64()),
                        pa.field("effective_time", pa.string()),
                        pa.field("raw_data", pa.string()),
                        pa.field("source_filename", pa.string()),
                        pa.field("loaded_at", pa.string()),
                    ]
                )

            table = pa.Table.from_pylist(
                [dict(zip(schema.names, row, strict=False)) for row in rows],
                schema=schema,
            )
            pq.write_table(table, filepath)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_bulk_load_and_merge_formats(
    sqlite_loader: SqliteLoader, tmp_path: Path, file_format: str
) -> None:
    """
    A more robust test for bulk loading and merging, parameterized
    for both CSV and Parquet formats. This validates the new Parquet path.
    """
    # 1. Arrange
    doc_id = str(uuid4())
    set_id = str(uuid4())
    test_data: dict[str, list[tuple[Any, ...]]] = {
        "spl_raw_documents": [
            (
                doc_id,
                set_id,
                1,
                "2025-01-01",
                '{"key":"value"}',
                "file.zip",
                "2025-01-01T12:00:00Z",
            )
        ],
        "products": [
            (
                doc_id,
                set_id,
                1,
                "2025-01-01",
                "Parquet Product",
                "Arrow Inc.",
                "TABLET",
                "ORAL",
                1,
                "2025-01-01T12:00:00Z",
            )
        ],
        "ingredients": [
            (doc_id, "Parquet Ingredient", "UNII-ARROW", "500", "1", "mg", 1)
        ],
    }
    _create_intermediate_files(tmp_path, file_format, test_data)

    # 2. Act
    intermediate_dir = tmp_path / "intermediate"
    sqlite_loader.bulk_load_to_staging(intermediate_dir)
    sqlite_loader.merge_from_staging(mode="full-load")

    # 3. Assert
    with sqlite_loader._get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT product_name FROM products WHERE document_id = ?", (doc_id,)
        )
        res = cur.fetchone()
        assert res is not None
        assert res[0] == "Parquet Product"

        cur.execute(
            "SELECT ingredient_name FROM ingredients WHERE document_id = ?", (doc_id,)
        )
        res = cur.fetchone()
        assert res is not None
        assert res[0] == "Parquet Ingredient"

        # Check that staging tables are empty after a merge
        cur.execute("SELECT count(*) FROM products_staging")
        assert cur.fetchone()[0] == 0


def test_delta_merge_updates_is_latest_version_correctly(
    sqlite_loader: SqliteLoader, tmp_path: Path
) -> None:
    """Test that delta merge correctly updates the is_latest_version flag."""
    with sqlite_loader._get_conn() as conn:
        conn.execute(
            "INSERT INTO spl_raw_documents (document_id, set_id) "
            "VALUES ('doc1-v1', 'set1')"
        )
        conn.execute(
            "INSERT INTO products "
            "(document_id, set_id, version_number, is_latest_version) "
            "VALUES ('doc1-v1', 'set1', 1, 1)"
        )
        conn.commit()

    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    (intermediate_dir / "spl_raw_documents.csv").write_text(
        'doc1-v2,set1,2,2025-02-01,{"key":"v2"},f2.zip,2025-02-01T12:00:00\n'
        'doc2-v1,set2,1,2025-03-01,{"key":"v1"},f3.zip,2025-03-01T12:00:00\n'
    )
    (intermediate_dir / "products.csv").write_text(
        "doc1-v2,set1,2,2025-02-01,Product A v2,Pfizer,Capsule,Oral,1,"
        "2025-02-01T12:00:00\n"
        "doc2-v1,set2,1,2025-03-01,Product B,Moderna,Injection,Parenteral,1,"
        "2025-03-01T12:00:00\n"
    )
    sqlite_loader.bulk_load_to_staging(intermediate_dir)
    sqlite_loader.merge_from_staging(mode="delta-load")

    with sqlite_loader._get_conn() as conn:
        res = conn.execute(
            "SELECT is_latest_version FROM products WHERE document_id = 'doc1-v1'"
        ).fetchone()
        assert res[0] == 0
        res = conn.execute(
            "SELECT is_latest_version FROM products WHERE document_id = 'doc1-v2'"
        ).fetchone()
        assert res[0] == 1
        res = conn.execute(
            "SELECT product_name FROM products WHERE document_id = 'doc2-v1'"
        ).fetchone()
        assert res[0] == "Product B"


def test_optimizations_are_applied_for_full_load(db_settings: SqliteSettings) -> None:
    """Test optimizations are correctly applied for full loads."""
    loader = SqliteLoader(db_settings)
    loader.initialize_schema()

    with loader._get_conn() as conn:
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1

    loader.pre_load_optimization(mode="delta-load")
    with loader._get_conn() as conn:
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1

    loader.pre_load_optimization(mode="full-load")
    with loader._get_conn() as conn:
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 0

    loader.post_load_cleanup(mode="full-load")
    with loader._get_conn() as conn:
        assert conn.execute("PRAGMA foreign_keys;").fetchone()[0] == 1

    loader.close_conn()
