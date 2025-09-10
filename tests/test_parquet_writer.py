import json
import tempfile
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import xmltodict

from py_load_spl.models import Product, SplRawDocument
from py_load_spl.transformation import ParquetWriter


def test_parquet_writer_writes_correct_data():
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
            effective_time="20250909",
            product_name="Test Drug 1",
            manufacturer_name="Test Pharma",
            is_latest_version=True,
        )

        doc_id_2 = uuid4()
        raw_spl_doc = SplRawDocument(
            document_id=doc_id_2,
            set_id=uuid4(),
            version_number=2,
            effective_time="20250910",
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
