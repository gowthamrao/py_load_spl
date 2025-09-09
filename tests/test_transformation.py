import csv
from pathlib import Path
from uuid import UUID

from py_load_spl.transformation import Transformer

# A sample record mimicking the output of the parsing stage for one SPL file.
# Based on the structure from sample_spl.xml
SAMPLE_PARSED_RECORD = {
    "document_id": "d1b64b62-050a-4895-924c-d2862d2a6a69",
    "set_id": "a2c3b6f0-a38f-4b48-96eb-3b2b403816a4",
    "version_number": 1,
    "effective_time": "20250907",
    "product_name": "Jules's Sample Drug",
    "manufacturer_name": "Jules Pharmaceuticals",
    "dosage_form": "TABLET",
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
        {"marketing_category": "active", "start_date": "20250101"}
    ],
}


def test_transformer_creates_correct_csvs(tmp_path: Path) -> None:
    """
    Tests that the Transformer correctly processes a parsed record
    and writes the data to the correct CSV files in the expected format.
    """
    # 1. Arrange
    output_dir = tmp_path / "test_output"
    parsed_data_stream = [SAMPLE_PARSED_RECORD]
    transformer = Transformer(output_dir=output_dir)

    # 2. Act
    stats = transformer.transform_stream(parsed_data_stream)

    # 3. Assert
    # Assert that the stats are correct
    assert isinstance(stats, dict)
    assert stats.get("products.csv") == 1
    assert stats.get("ingredients.csv") == 1
    assert stats.get("packaging.csv") == 1
    assert stats.get("marketing_status.csv") == 1
    assert stats.get("product_ndcs.csv") == 1
    assert stats.get("spl_raw_documents.csv") == 1
    assert sum(stats.values()) == 6

    # Check that all expected files were created
    products_csv = output_dir / "products.csv"
    ingredients_csv = output_dir / "ingredients.csv"
    packaging_csv = output_dir / "packaging.csv"
    marketing_status_csv = output_dir / "marketing_status.csv"
    product_ndcs_csv = output_dir / "product_ndcs.csv"
    spl_raw_documents_csv = output_dir / "spl_raw_documents.csv"

    assert products_csv.exists()
    assert ingredients_csv.exists()
    assert packaging_csv.exists()
    assert marketing_status_csv.exists()
    assert product_ndcs_csv.exists()
    assert spl_raw_documents_csv.exists()

    # Verify the content of products.csv
    with open(products_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        product_row = rows[0]
        assert product_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert product_row[4] == "Jules's Sample Drug"

    # Verify the content of spl_raw_documents.csv
    with open(spl_raw_documents_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        raw_doc_row = rows[0]
        assert raw_doc_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert raw_doc_row[4] == '"<xml>some fake raw data</xml>"'
        assert raw_doc_row[5] == "sample.xml"

    # Verify the content of product_ndcs.csv
    with open(product_ndcs_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        ndc_row = rows[0]
        assert ndc_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert ndc_row[1] == "12345-678"

    # Verify the content of packaging.csv
    with open(packaging_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        packaging_row = rows[0]
        assert packaging_row[1] == "12345-678-90"
        assert packaging_row[2] == "30 Tablets in 1 Bottle"
        assert packaging_row[3] == "BOTTLE"
