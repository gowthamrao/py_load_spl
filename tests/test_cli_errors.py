import pytest
from typer.testing import CliRunner
from unittest.mock import MagicMock

from py_load_spl.cli import app
from py_load_spl.config import Settings

runner = CliRunner()

def test_cli_no_command():
    """Tests that a helpful message is shown when no command is specified."""
    result = runner.invoke(app, [])
    # Typer returns 0 and prints help, which is fine.
    assert result.exit_code == 0
    assert "No command specified" in result.stdout

def test_cli_unsupported_db_adapter(monkeypatch):
    """Tests that the CLI exits gracefully with an unsupported DB adapter."""
    # We must mock get_settings to bypass Pydantic's validation for the test
    mock_settings = Settings(db={"adapter": "unsupported_db"})
    monkeypatch.setattr("py_load_spl.cli.get_settings", lambda: mock_settings)

    result = runner.invoke(app, ["init"])
    assert result.exit_code == 1
    assert "Unsupported DB adapter 'unsupported_db'" in result.stdout

def test_full_load_non_existent_source():
    """Tests the validation for a source path that does not exist."""
    result = runner.invoke(app, ["full-load", "--source", "/non/existent/path"])
    assert result.exit_code == 1
    assert "Source path '/non/existent/path' does not exist" in result.stdout

def test_init_db_failure(monkeypatch):
    """Tests that a failure during schema initialization is handled."""
    # Mock the loader instance to raise an error on initialize_schema
    mock_loader_instance = MagicMock()
    mock_loader_instance.initialize_schema.side_effect = Exception("DB init failed")

    # Mock get_db_loader to return our faulty loader
    monkeypatch.setattr("py_load_spl.cli.get_db_loader", lambda s: mock_loader_instance)

    result = runner.invoke(app, ["init"])
    assert result.exit_code == 1
    assert "Schema initialization failed: DB init failed" in result.stdout

def test_full_load_no_xml_files_found(tmp_path, monkeypatch):
    """Tests that a full load on an empty directory aborts gracefully."""
    # Mock the loader to avoid DB connection
    monkeypatch.setattr("py_load_spl.cli.get_db_loader", lambda s: MagicMock())
    result = runner.invoke(app, ["full-load", "--source", str(tmp_path)])
    assert result.exit_code == 0
    assert "No XML files found in the source. Aborting." in result.stdout


def test_run_full_load_catches_exception(tmp_path, monkeypatch):
    """Tests that the main try/except block in _run_full_load handles errors."""
    # Create a fake XML file to start the process
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    # A minimal valid XML to get past the parsing stage
    (source_dir / "test.xml").write_text(
        '<document xmlns="urn:hl7-org:v3"><id root="a"/></document>'
    )

    # Mock the loader to fail during the merge step
    mock_loader_instance = MagicMock()
    mock_loader_instance.merge_from_staging.side_effect = Exception("Merge Failed!")
    monkeypatch.setattr("py_load_spl.cli.get_db_loader", lambda s: mock_loader_instance)

    result = runner.invoke(app, ["full-load", "--source", str(source_dir)])
    assert result.exit_code == 1
    assert "An error occurred during the full load process: Merge Failed!" in result.stdout
