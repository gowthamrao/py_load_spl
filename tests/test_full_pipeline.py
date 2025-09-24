import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from py_load_spl.config import PostgresSettings
from py_load_spl.db.postgres import PostgresLoader
from py_load_spl.parsing import parse_spl_file
from py_load_spl.transformation import CsvWriter, Transformer

SAMPLE_XML_WITH_ROUTE = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250907" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Sample Drug</name>
        <formCode code="C42916" displayName="TABLET" />
        <routeCode code="C38288" displayName="ORAL" />
        <asEntityWithGeneric>
          <genericMedicine>
            <name>SAMPLAMYCIN</name>
          </genericMedicine>
        </asEntityWithGeneric>
        <ingredient classCode="ACT">
          <quantity>
            <numerator value="100" unit="mg" />
            <denominator value="1" unit="TABLET" />
          </quantity>
          <ingredientSubstance>
            <name>SAMPLESTAT</name>
            <code code="UNII-SAMPLE" displayName="SAMPLESTAT" />
          </ingredientSubstance>
        </ingredient>
      </manufacturedProduct>
      <manufacturer>
        <name>Sample Pharmaceuticals</name>
      </manufacturer>
    </manufacturedProduct>
  </subject>
  <component>
    <structuredBody>
      <component>
        <section ID="s2">
          <code code="51945-4" displayName="PACKAGE LABEL.PRINCIPAL DISPLAY PANEL" />
          <text>
            Some text here that the old parser might have used.
          </text>
          <component>
            <section>
                <part>
                  <code code="12345-678-90" displayName="NDC" />
                  <name>30 Tablets in 1 Bottle</name>
                  <formCode code="C43182" displayName="BOTTLE" />
                </part>
            </section>
          </component>
          <subject>
            <marketingAct>
              <statusCode code="active"/>
              <effectiveTime>
                <low value="20250101"/>
              </effectiveTime>
            </marketingAct>
          </subject>
        </section>
      </component>
    </structuredBody>
  </component>
</document>
"""


@patch("py_load_spl.db.postgres.psycopg2")
def test_full_etl_pipeline_mocked(
    mock_psycopg2: MagicMock, mocker: MockerFixture
) -> None:
    """
    Tests the full ETL pipeline with a mocked database to avoid docker issues.
    Verifies that the correct data is generated and that the loader methods
    are called as expected.
    """
    with (
        tempfile.TemporaryDirectory() as source_dir_str,
        tempfile.TemporaryDirectory() as output_dir_str,
    ):
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

        db_settings = PostgresSettings(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            name="test",
            adapter="postgresql",
        )
        loader = PostgresLoader(db_settings)
        # Since we are testing the pipeline, we need to mock the new methods
        mocker.patch.object(loader, "start_run", return_value=1)
        mocker.patch.object(loader, "end_run")

        # 2. Act: Run the E-T-L process
        # Mimic the parallel execution logic from the CLI
        xml_files = list(source_dir.glob("*.xml"))
        parsed_stream = map(parse_spl_file, xml_files)

        writer = CsvWriter(output_dir)
        transformer = Transformer(writer)
        stats = transformer.transform_stream(parsed_stream)

        # Mock the return value for the new row count feature
        mocker.patch.object(
            loader, "bulk_load_to_staging", return_value=sum(stats.values())
        )

        loader.initialize_schema()
        loaded_count = loader.bulk_load_to_staging(output_dir)
        # Manually check the validation logic
        assert loaded_count == sum(stats.values())
        loader.merge_from_staging(mode="full-load")

        # 3. Assert
        # Assert that the stats are returned correctly
        assert stats["products.csv"] == 1
        assert stats["spl_raw_documents.csv"] == 1

        # Assert that the CSV was created correctly with the new field
        products_csv = output_dir / "products.csv"
        assert products_csv.exists()
        with open(products_csv) as f:
            content = f.read()
            # The order of columns in the model is: document_id, set_id, version_number,
            # effective_time, product_name, manufacturer_name, dosage_form, route_of_administration
            assert "ORAL" in content
            assert "d1b64b62-050a-4895-924c-d2862d2a6a69" in content
            print("Verified 'ORAL' is present in the output products.csv")

        # Assert that the database methods were called
        mock_psycopg2.connect.assert_called()
        mock_conn.cursor.assert_called()
        # Check that initialize_schema was called
        assert mock_cur.execute.call_count > 0
        # Check that bulk_load_to_staging was called
        loader.bulk_load_to_staging.assert_called_with(output_dir)  # type: ignore
        # Check that merge_from_staging was called (via execute)
        # It should truncate tables and then insert into them
        truncate_calls = [
            call
            for call in mock_cur.execute.call_args_list
            if "TRUNCATE" in call.args[0]
        ]
        insert_calls = [
            call
            for call in mock_cur.execute.call_args_list
            if "INSERT INTO" in call.args[0]
        ]
        assert len(truncate_calls) > 0
        assert len(insert_calls) > 0
        print("Verified that database loader methods were called.")


@patch("py_load_spl.main.get_db_loader")
@patch("py_load_spl.main.get_file_writer")
@patch("py_load_spl.main.Transformer")
@patch("py_load_spl.main.concurrent.futures.ProcessPoolExecutor")
def test_record_count_validation(
    mock_executor: MagicMock,
    mock_transformer: MagicMock,
    mock_writer_getter: MagicMock,
    mock_loader_getter: MagicMock,
) -> None:
    """
    Unit test for the record count validation logic in `run_full_load`.
    """
    from py_load_spl.main import run_full_load

    # Arrange: Mock all the components
    mock_loader = MagicMock()
    mock_writer = MagicMock()
    mock_transformer_instance = MagicMock()

    mock_loader_getter.return_value = mock_loader
    mock_writer_getter.return_value = mock_writer
    mock_transformer.return_value = mock_transformer_instance

    # This is the count that the transformer will report
    transformed_count = 100
    mock_transformer_instance.transform_stream.return_value = {"products.csv": 100}

    # Create a dummy source directory with a fake file
    with tempfile.TemporaryDirectory() as source_dir_str:
        source_dir = Path(source_dir_str)
        (source_dir / "dummy.xml").touch()

        # --- Test Case 1: Counts match (Success) ---
        mock_loader.bulk_load_to_staging.return_value = transformed_count

        # Act
        run_full_load(settings=MagicMock(), source=source_dir)

        # Assert
        mock_loader.end_run.assert_called_with(
            mock_loader.start_run.return_value, "SUCCESS", transformed_count, None
        )

        # --- Test Case 2: Counts mismatch (Failure) ---
        mismatched_count = 99
        mock_loader.bulk_load_to_staging.return_value = mismatched_count

        # Act & Assert
        with pytest.raises(RuntimeError) as excinfo:
            run_full_load(settings=MagicMock(), source=source_dir)

        assert "Data integrity check failed" in str(excinfo.value)
        assert "100" in str(excinfo.value)
        assert "99" in str(excinfo.value)
        mock_loader.end_run.assert_called_with(
            mock_loader.start_run.return_value, "FAILED", 0, str(excinfo.value)
        )


@pytest.mark.integration
def test_full_load_pipeline_with_postgres_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    A true end-to-end integration test for the 'full-load' command.
    - Spins up a real PostgreSQL database in a Docker container.
    - Creates a sample SPL XML file.
    - Runs the 'full-load' CLI command against the test database.
    - Connects to the database to verify the data was loaded correctly.
    """
    import psycopg2
    from testcontainers.postgres import PostgresContainer
    from typer.testing import CliRunner

    from py_load_spl.cli import app

    runner = CliRunner()

    container = PostgresContainer("postgres:15-alpine")
    container.waiting_for(
        LogMessageWaitStrategy(
            "database system is ready to accept connections", times=1
        )
    )
    with container as postgres_container:
        # Get connection details from the container
        db_settings = postgres_container.get_connection_url()
        db_name = db_settings.split("/")[-1]
        db_user = postgres_container.username
        db_password = postgres_container.password
        db_host = postgres_container.get_container_host_ip()
        db_port = postgres_container.get_exposed_port(5432)

        # Set environment variables for the CLI app to use
        monkeypatch.setenv("DB_ADAPTER", "postgresql")
        monkeypatch.setenv("DB_HOST", db_host)
        monkeypatch.setenv("DB_PORT", str(db_port))
        monkeypatch.setenv("DB_USER", db_user)
        monkeypatch.setenv("DB_PASSWORD", db_password)
        monkeypatch.setenv("DB_NAME", db_name)

        # 1. Arrange: Initialize the schema
        result_init = runner.invoke(app, ["init"])
        assert result_init.exit_code == 0
        assert "Schema initialization complete" in result_init.stdout

        # Create a temporary directory with a sample XML file
        with tempfile.TemporaryDirectory() as source_dir_str:
            source_dir = Path(source_dir_str)
            xml_file = source_dir / "sample.xml"
            # Using the same sample XML from the mocked test
            xml_file.write_text(SAMPLE_XML_WITH_ROUTE)

            # 2. Act: Run the full-load command
            result_load = runner.invoke(app, ["full-load", "--source", str(source_dir)])
            if result_load.exit_code != 0:
                print(f"CLI Error Output:\n{result_load.stdout}")
            assert result_load.exit_code == 0
            assert "Full load process finished successfully" in result_load.stdout

            # 3. Assert: Connect to the database and verify the data
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                user=db_user,
                password=db_password,
                dbname=db_name,
            )
            cur = conn.cursor()

            # Check products table
            cur.execute("SELECT COUNT(*) FROM products;")
            count_result = cur.fetchone()
            assert count_result is not None
            assert count_result[0] == 1
            cur.execute(
                "SELECT product_name, dosage_form, route_of_administration FROM products;"
            )
            product_row = cur.fetchone()
            assert product_row is not None
            assert product_row[0] == "Sample Drug"
            assert product_row[1] == "TABLET"
            assert product_row[2] == "ORAL"

            # Check spl_raw_documents table
            cur.execute("SELECT COUNT(*) FROM spl_raw_documents;")
            count_result = cur.fetchone()
            assert count_result is not None
            assert count_result[0] == 1

            cur.close()
            conn.close()
