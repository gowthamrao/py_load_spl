import csv
from pathlib import Path
from uuid import UUID

import pyarrow.parquet as pq
import pytest
from pytest_mock import MockerFixture

from py_load_spl.transformation import (
    CsvWriter,
    FileWriter,
    ParquetWriter,
    Transformer,
)

# A sample record mimicking the output of the parsing stage for one SPL file.
# Based on the structure from sample_spl.xml
SAMPLE_PARSED_RECORD = {
    "document_id": UUID("d1b64b62-050a-4895-924c-d2862d2a6a69"),
    "set_id": UUID("a2c3b6f0-a38f-4b48-96eb-3b2b403816a4"),
    "version_number": 1,
    "effective_time": "20250907",
    "product_name": "Jules's Sample Drug",
    "manufacturer_name": "Jules Pharmaceuticals",
    "dosage_form": "TABLET",
    "route_of_administration": "ORAL",
    "is_latest_version": True,
    "loaded_at": "2025-09-08T12:00:00Z",
    "raw_data": "<xml>some fake raw data</xml>",
    "source_filename": "sample.xml",
    "product_ndcs": [{"ndc_code": "12345-678"}],
    "ingredients": [
        {
            "ingredient_name": "JULESTAT",
            "substance_code": "UNII-JULE",
            "is_active_ingredient": True,
            "strength_numerator": "100",
            "strength_denominator": "1",
            "unit_of_measure": "mg",
        }
    ],
    "packaging": [
        {
            "package_ndc": "12345-678-90",
            "package_description": "30 Tablets in 1 Bottle",
            "package_type": "BOTTLE",
        }
    ],
    "marketing_status": [
        {"marketing_category": "active", "start_date": "20250101", "end_date": None}
    ],
}


@pytest.mark.parametrize(
    "writer_class, file_ext", [(CsvWriter, ".csv"), (ParquetWriter, ".parquet")]
)
def test_transformer_with_different_writers(
    tmp_path: Path, writer_class: type[FileWriter], file_ext: str
) -> None:
    """
    Tests that the Transformer correctly processes a parsed record and writes
    the data using different writer implementations (CSV and Parquet).
    """
    # 1. Arrange
    output_dir = tmp_path / "test_output"
    parsed_data_stream = [SAMPLE_PARSED_RECORD]
    writer = writer_class(output_dir)
    transformer = Transformer(writer=writer)

    # 2. Act
    stats = transformer.transform_stream(parsed_data_stream)

    # 3. Assert stats and file existence
    assert isinstance(stats, dict)
    assert stats.get(f"products{file_ext}") == 1
    assert stats.get(f"ingredients{file_ext}") == 1
    assert sum(stats.values()) == 6

    products_file = output_dir / f"products{file_ext}"
    ingredients_file = output_dir / f"ingredients{file_ext}"
    packaging_file = output_dir / f"packaging{file_ext}"

    assert products_file.exists()
    assert ingredients_file.exists()
    assert packaging_file.exists()

    # 4. Assert file content based on format
    if file_ext == ".csv":
        # Verify the content of products.csv
        with open(products_file) as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 1
            product_row = rows[0]
            assert product_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
            assert product_row[4] == "Jules's Sample Drug"
    elif file_ext == ".parquet":
        # Verify the content of products.parquet
        table = pq.read_table(products_file)
        assert table.num_rows == 1
        data = table.to_pylist()
        product_row = data[0]
        assert product_row["document_id"] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert product_row["product_name"] == "Jules's Sample Drug"
        assert product_row["dosage_form"] == "TABLET"
        assert product_row["is_latest_version"] is True

        # Verify the content of ingredients.parquet
        ing_table = pq.read_table(ingredients_file)
        assert ing_table.num_rows == 1
        ing_data = ing_table.to_pylist()
        ing_row = ing_data[0]
        assert ing_row["document_id"] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert ing_row["ingredient_name"] == "JULESTAT"
        assert ing_row["is_active_ingredient"] is True


def test_parquet_writer_batching(tmp_path: Path, mocker: MockerFixture) -> None:
    """
    Tests that the ParquetWriter correctly writes data in batches to manage memory.
    """
    # 1. Arrange
    output_dir = tmp_path / "test_output"
    # Use a small batch size to easily trigger the flushing mechanism
    writer = ParquetWriter(output_dir, batch_size=2)
    transformer = Transformer(writer=writer)

    # Create 3 records. With a batch size of 2, this should trigger one flush
    # during the write operations, and one flush on close.
    record2 = SAMPLE_PARSED_RECORD.copy()
    record2["document_id"] = UUID("a" * 32)
    record3 = SAMPLE_PARSED_RECORD.copy()
    record3["document_id"] = UUID("b" * 32)
    parsed_data_stream = [SAMPLE_PARSED_RECORD, record2, record3]

    # Spy on the method that writes to the file
    spy = mocker.spy(writer, "_flush_batch")

    # 2. Act
    transformer.transform_stream(parsed_data_stream)

    # 3. Assert
    # _flush_batch should be called once for 'products' when the 2nd record is processed,
    # and then once for each table with remaining data when the writer is closed.
    assert spy.call_count > 1

    # Check the call for the 'products' table specifically.
    # It should have been called once for the first batch, and once on close.
    products_flush_calls = [
        call for call in spy.call_args_list if call.args[0] == "products"
    ]
    assert len(products_flush_calls) == 2

    # Verify the final file contains all records
    products_file = output_dir / "products.parquet"
    assert products_file.exists()
    table = pq.read_table(products_file)
    assert table.num_rows == 3

    # Verify document_ids to ensure all records are present
    doc_ids = {str(row["document_id"]) for row in table.to_pylist()}
    assert doc_ids == {
        "d1b64b62-050a-4895-924c-d2862d2a6a69",
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    }
