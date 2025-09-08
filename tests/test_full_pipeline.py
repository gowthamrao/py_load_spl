import pytest
from unittest.mock import MagicMock, patch
import tempfile
from pathlib import Path
import os

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader
from py_load_spl.parsing import iter_spl_files
from py_load_spl.transformation import Transformer

SAMPLE_XML_WITH_ROUTE = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250907" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Jules's Sample Drug</name>
        <formCode code="C42916" displayName="TABLET" />
        <routeCode code="C38288" displayName="ORAL" />
        <asEntityWithGeneric>
          <genericMedicine>
            <name>JULAMYCIN</name>
          </genericMedicine>
        </asEntityWithGeneric>
      </manufacturedProduct>
    </manufacturedProduct>
  </subject>
</document>
"""


@patch("py_load_spl.db.postgres.psycopg2")
def test_full_etl_pipeline_mocked(mock_psycopg2):
    """
    Tests the full ETL pipeline with a mocked database to avoid docker issues.
    Verifies that the correct data is generated and that the loader methods
    are called as expected.
    """
    with tempfile.TemporaryDirectory() as source_dir_str, tempfile.TemporaryDirectory() as output_dir_str:
        source_dir = Path(source_dir_str)
        output_dir = Path(output_dir_str)

        # 1. Arrange: Create a sample XML file and set up mocks
        xml_file = source_dir / "sample.xml"
        xml_file.write_text(SAMPLE_XML_WITH_ROUTE)

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        # Ensure the cursor() context manager returns our mock cursor
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        db_settings = DatabaseSettings(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            name="test",
            adapter="postgresql",
        )
        loader = PostgresLoader(db_settings)
        # Since we are testing the pipeline, we need to mock the new methods
        loader.start_run = MagicMock(return_value=1)
        loader.end_run = MagicMock()


        # 2. Act: Run the E-T-L process
        parsed_stream = iter_spl_files(source_dir)
        transformer = Transformer(output_dir)
        transformer.transform_stream(parsed_stream)

        loader.initialize_schema()
        loader.bulk_load_to_staging(output_dir)
        loader.merge_from_staging(mode="full-load")

        # 3. Assert
        # Assert that the CSV was created correctly with the new field
        products_csv = output_dir / "products.csv"
        assert products_csv.exists()
        with open(products_csv, "r") as f:
            content = f.read()
            # The order of columns in the model is: document_id, set_id, version_number,
            # effective_time, product_name, manufacturer_name, dosage_form, route_of_administration
            assert 'ORAL' in content
            assert 'd1b64b62-050a-4895-924c-d2862d2a6a69' in content
            print("Verified 'ORAL' is present in the output products.csv")

        # Assert that the database methods were called
        mock_psycopg2.connect.assert_called()
        mock_conn.cursor.assert_called()
        # Check that initialize_schema was called
        assert mock_cur.execute.call_count > 0
        # Check that bulk_load_to_staging was called (via copy_expert)
        assert mock_cur.copy_expert.call_count > 0
        # Check that merge_from_staging was called (via execute)
        # It should truncate tables and then insert into them
        truncate_calls = [call for call in mock_cur.execute.call_args_list if "TRUNCATE" in call[0][0]]
        insert_calls = [call for call in mock_cur.execute.call_args_list if "INSERT INTO" in call[0][0]]
        assert len(truncate_calls) > 0
        assert len(insert_calls) > 0
        print("Verified that database loader methods were called.")
