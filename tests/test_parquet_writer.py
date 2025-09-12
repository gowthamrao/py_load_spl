import json
import tempfile
from datetime import date
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import xmltodict

from py_load_spl import schemas
from py_load_spl.models import Product, SplRawDocument
from py_load_spl.transformation import ParquetWriter


def test_parquet_writer_uses_explicit_schema(tmp_path: Path) -> None:
    """
    Tests that the ParquetWriter uses the predefined, explicit schema when
    writing files, ensuring data integrity and correct types.
    This is the primary test for the changes made to use explicit schemas.
    """
    # 1. Arrange
    writer = ParquetWriter(output_dir=tmp_path)
    doc_id = uuid4()
    set_id = uuid4()

    # Create a product instance with all data types to test schema enforcement
    product = Product(
        document_id=doc_id,
        set_id=set_id,
        version_number=5,
        effective_time=date(2025, 9, 10),
        product_name="Schema Test Drug",
        manufacturer_name="Schema Pharma Inc.",
        dosage_form="CAPSULE",
        route_of_administration="ORAL",
        is_latest_version=True,
    )

    # 2. Act
    with writer:
        writer.write(product)

    # 3. Assert
    output_file = tmp_path / "products.parquet"
    assert output_file.exists()

    # Read the table and, most importantly, its schema
    table = pq.read_table(output_file)

    # Assert that the schema of the written file is exactly what we defined
    assert table.schema == schemas.PRODUCT_SCHEMA

    # Assert that the data was written correctly and can be read back
    assert table.num_rows == 1
    data = table.to_pydict()
    assert data["document_id"][0] == str(doc_id)
    assert data["set_id"][0] == str(set_id)
    assert data["version_number"][0] == 5
    assert data["effective_time"][0] == date(2025, 9, 10)
    assert data["product_name"][0] == "Schema Test Drug"
    assert data["is_latest_version"][0] is True
    # loaded_at is a datetime, check that it's a valid datetime object
    assert data["loaded_at"][0] is not None


def test_parquet_writer_writes_correct_data() -> None:
    """
    Tests that ParquetWriter correctly writes different models to separate files
    and handles data types like UUID and nested JSON correctly.
    """
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        writer = ParquetWriter(output_dir=temp_dir)

        # --- Test Data ---
        doc_id_1 = uuid4()
        product_1 = Product(
            document_id=doc_id_1,
            set_id=uuid4(),
            version_number=1,
            effective_time=date(2025, 9, 9),
            product_name="Test Drug 1",
            manufacturer_name="Test Pharma",
            is_latest_version=True,
        )

        doc_id_2 = uuid4()
        raw_spl_doc = SplRawDocument(
            document_id=doc_id_2,
            set_id=uuid4(),
            version_number=2,
            effective_time=date(2025, 9, 10),
            raw_data="<section><title>Warnings</title><text>May cause drowsiness.</text></section>",
            source_filename="test.zip/test.xml",
        )

        # --- Transform Data ---
        # Manually transform the raw_spl_doc to JSON, since the test
        # bypasses the Transformer class.
        if raw_spl_doc.raw_data:
            xml_dict = xmltodict.parse(raw_spl_doc.raw_data)
            raw_spl_doc.raw_data = json.dumps(xml_dict)

        # --- Write Data ---
        with writer:
            writer.write(product_1)
            writer.write(raw_spl_doc)

        # --- Verification ---
        # 1. Check product data
        product_file = temp_dir / "products.parquet"
        assert product_file.exists()
        product_table = pq.read_table(product_file)
        assert product_table.num_rows == 1
        product_data = product_table.to_pydict()
        assert product_data["document_id"][0] == str(doc_id_1)
        assert product_data["product_name"][0] == "Test Drug 1"
        assert product_data["is_latest_version"][0] is True

        # 2. Check raw SPL document data
        raw_spl_file = temp_dir / "spl_raw_documents.parquet"
        assert raw_spl_file.exists()
        raw_spl_table = pq.read_table(raw_spl_file)
        assert raw_spl_table.num_rows == 1
        raw_spl_data = raw_spl_table.to_pydict()
        assert raw_spl_data["document_id"][0] == str(doc_id_2)
        # Verify the fix: raw_data should be a JSON string
        raw_data_json = raw_spl_data["raw_data"][0]
        assert isinstance(raw_data_json, str)
        assert json.loads(raw_data_json) == {
            "section": {"title": "Warnings", "text": "May cause drowsiness."}
        }
        assert raw_spl_data["source_filename"][0] == "test.zip/test.xml"

        # 3. Check stats
        assert writer.stats["products.parquet"] == 1
        assert writer.stats["spl_raw_documents.parquet"] == 1
