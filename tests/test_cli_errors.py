import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from py_load_spl.cli import app
from py_load_spl.config import Settings

runner = CliRunner()


def test_cli_no_command() -> None:
    """Tests that a helpful message is shown when no command is specified."""
    result = runner.invoke(app, [])
    # Typer returns 0 and prints help, which is fine.
    assert result.exit_code == 0
    assert "No command specified" in result.stdout


def test_cli_unsupported_db_adapter() -> None:
    """
    Tests that creating Settings with an unsupported DB adapter raises a
    ValidationError, thanks to Pydantic's Literal validation.
    """
    with pytest.raises(ValidationError) as excinfo:
        Settings(db={"adapter": "unsupported_db"})  # type: ignore

    # Check that the error message is informative
    error_str = str(excinfo.value)
    assert "Input tag 'unsupported_db' found" in error_str
    assert "does not match any of the expected" in error_str


def test_full_load_non_existent_source() -> None:
    """Tests the validation for a source path that does not exist."""
    result = runner.invoke(app, ["full-load", "--source", "/non/existent/path"])
    assert result.exit_code == 1
    assert "Source path '/non/existent/path' does not exist" in result.stdout


def test_init_db_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests that a failure during schema initialization is handled."""
    # Mock the loader instance to raise an error on initialize_schema
    mock_loader_instance = MagicMock()
    mock_loader_instance.initialize_schema.side_effect = Exception("DB init failed")

    # Mock get_db_loader to return our faulty loader
    monkeypatch.setattr("py_load_spl.cli.get_db_loader", lambda s: mock_loader_instance)

    result = runner.invoke(app, ["init"])
    assert result.exit_code == 1
    assert "Schema initialization failed: DB init failed" in result.stdout


def test_full_load_no_xml_files_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that a full load on an empty directory aborts gracefully."""
    # Mock the loader to avoid DB connection
    monkeypatch.setattr("py_load_spl.main.get_db_loader", lambda s: MagicMock())
    result = runner.invoke(
        app, ["--log-format", "text", "full-load", "--source", str(tmp_path)]
    )
    assert result.exit_code == 0
    assert "No XML files found in the source. Aborting." in result.stdout


def test_run_full_load_catches_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that the main try/except block in _run_full_load handles errors."""
    # Create a fake XML file to start the process
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    # A minimal valid XML to get past the parsing stage
    doc_id = str(uuid.uuid4())
    set_id = str(uuid.uuid4())
    (source_dir / "test.xml").write_text(
        f"""
        <document xmlns="urn:hl7-org:v3">
            <id root="{doc_id}"/>
            <setId root="{set_id}"/>
            <effectiveTime value="20240101"/>
        </document>
        """
    )

    # Mock the loader to fail during the merge step
    mock_loader_instance = MagicMock()
    mock_loader_instance.merge_from_staging.side_effect = Exception("Merge Failed!")
    mock_loader_instance.bulk_load_to_staging.return_value = 2
    monkeypatch.setattr(
        "py_load_spl.main.get_db_loader", lambda s: mock_loader_instance
    )

    result = runner.invoke(
        app,
        [
            "--log-format",
            "text",
            "full-load",
            "--source",
            str(source_dir),
        ],
    )
    assert result.exit_code == 1
    assert (
        "An error occurred during the full load process: Merge Failed!" in result.stdout
    )
