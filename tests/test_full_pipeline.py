import pytest
from unittest.mock import MagicMock, patch
import tempfile
from pathlib import Path
import os
import psycopg2
from typer.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from py_load_spl.config import DatabaseSettings
from py_load_spl.db.postgres import PostgresLoader
from py_load_spl.parsing import iter_spl_files
from py_load_spl.transformation import Transformer
from py_load_spl.cli import app

# Mark all tests in this file as integration tests, unless otherwise specified
pytestmark = pytest.mark.integration


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


@pytest.mark.mocked
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


@pytest.fixture(scope="module")
def test_db_settings():
    """
    Spins up a PostgreSQL container and yields the connection settings.
    It also sets the environment variables so the CLI can pick them up.
    """
    with PostgresContainer("postgres:16-alpine") as postgres:
        settings = DatabaseSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
            user=postgres.username,
            password=postgres.password,
            name=postgres.dbname,
        )
        # Set env vars for the CLI runner
        os.environ["DB_HOST"] = settings.host
        os.environ["DB_PORT"] = str(settings.port)
        os.environ["DB_USER"] = settings.user
        os.environ["DB_PASSWORD"] = settings.password
        os.environ["DB_NAME"] = settings.name

        yield settings

        # Unset env vars
        del os.environ["DB_HOST"]
        del os.environ["DB_PORT"]
        del os.environ["DB_USER"]
        del os.environ["DB_PASSWORD"]
        del os.environ["DB_NAME"]


def test_full_load_command_integration(test_db_settings: DatabaseSettings):
    """
    Tests the full `init` and `full-load` CLI commands against a real database.
    """
    runner = CliRunner()

    with tempfile.TemporaryDirectory() as source_dir_str:
        source_dir = Path(source_dir_str)
        # Copy our test file into the temp dir
        test_xml = Path(__file__).parent / "test_spl_full.xml"
        source_file = source_dir / "test_spl_full.xml"
        source_file.write_text(test_xml.read_text())

        # 1. Run `init` command
        result_init = runner.invoke(app, ["init"])
        assert result_init.exit_code == 0
        assert "Schema initialization complete" in result_init.stdout

        # 2. Run `full-load` command
        result_load = runner.invoke(app, ["full-load", "--source", str(source_dir)])
        assert result_load.exit_code == 0
        assert "Full load process finished" in result_load.stdout

    # 3. Assert data in the database
        conn_params = {
            "host": test_db_settings.host,
            "port": test_db_settings.port,
            "user": test_db_settings.user,
            "password": test_db_settings.password,
            "dbname": test_db_settings.name,
        }
        conn = psycopg2.connect(**conn_params)
    with conn.cursor() as cur:
        # Check products table
        cur.execute("SELECT product_name FROM products WHERE document_id = 'd1b64b62-050a-4895-924c-d2862d2a6a69'")
        product_name = cur.fetchone()[0]
        assert product_name == "Jules's Sample Drug"

        # Check spl_raw_documents table
        # Cast the JSONB to text to get the raw XML string back
        cur.execute("SELECT raw_data::text FROM spl_raw_documents WHERE document_id = 'd1b64b62-050a-4895-924c-d2862d2a6a69'")
        # The result from postgres is a JSON string literal, e.g. '"<xml>..."',
        # so we strip the leading and trailing quotes.
        raw_xml = cur.fetchone()[0].strip('"')
        assert "<name>Jules's Sample Drug</name>" in raw_xml

        # Check product_ndcs table
        cur.execute("SELECT ndc_code FROM product_ndcs WHERE document_id = 'd1b64b62-050a-4895-924c-d2862d2a6a69'")
        ndc_code = cur.fetchone()[0]
        assert ndc_code == "12345-678"

        # Check packaging table
        cur.execute("SELECT package_ndc, package_description, package_type FROM packaging WHERE document_id = 'd1b64b62-050a-4895-924c-d2862d2a6a69' ORDER BY package_ndc")
        packages = cur.fetchall()
        assert len(packages) == 2
        assert packages[0] == ("12345-678-90", "100 TABLET in 1 BOTTLE", "BOTTLE")
        assert packages[1] == ("12345-678-91", "50 TABLET in 1 BLISTER PACK", "BLISTER PACK")
    conn.close()
