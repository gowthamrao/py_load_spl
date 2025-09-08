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
    "raw_xml": "<document>...</document>",
    "source_filename": "test.xml",
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
            "package_description": "100 TABLET in 1 BOTTLE",
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
    transformer.transform_stream(parsed_data_stream)

    # 3. Assert
    # Check that all expected files were created
    products_csv = output_dir / "products.csv"
    ingredients_csv = output_dir / "ingredients.csv"
    packaging_csv = output_dir / "packaging.csv"
    marketing_status_csv = output_dir / "marketing_status.csv"

    assert products_csv.exists()
    assert ingredients_csv.exists()
    assert packaging_csv.exists()
    assert marketing_status_csv.exists()

    # Verify the content of products.csv
    with open(products_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        product_row = rows[0]
        # Check a few key fields. Order is based on Pydantic model field order.
        assert product_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert product_row[2] == "1"  # version_number
        assert product_row[3] == "2025-09-07"  # effective_time (formatted)
        assert product_row[4] == "Jules's Sample Drug"
        assert product_row[6] == "TABLET"  # dosage_form
        assert product_row[7] == "\\N"  # route_of_administration (None)

    # Verify the content of ingredients.csv
    with open(ingredients_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        ingredient_row = rows[0]
        assert ingredient_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert ingredient_row[1] == "JULESTAT"
        assert ingredient_row[6] == "True" # is_active_ingredient

    # Verify the content of packaging.csv
    with open(packaging_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        packaging_row = rows[0]
        assert packaging_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert packaging_row[1] == "12345-678-90"
        assert packaging_row[2] == "100 TABLET in 1 BOTTLE"
        assert packaging_row[3] == "BOTTLE"

    # Verify the content of marketing_status.csv
    with open(marketing_status_csv) as f:
        reader = csv.reader(f)
        rows = list(reader)
        assert len(rows) == 1
        mkt_status_row = rows[0]
        assert mkt_status_row[0] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
        assert mkt_status_row[1] == "active"
        assert mkt_status_row[2] == "2025-01-01" # start_date (formatted)
        assert mkt_status_row[3] == "\\N" # end_date (None)
