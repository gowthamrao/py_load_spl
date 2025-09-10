from pathlib import Path

import pytest
from typer.testing import CliRunner

from py_load_spl.cli import app
from py_load_spl.config import Settings

runner = CliRunner()

# A valid XML sample that should process correctly
SAMPLE_XML_CONTENT_GOOD = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250909" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Good Drug</name>
      </manufacturedProduct>
    </manufacturedProduct>
  </subject>
</document>
"""

# An invalid XML sample that is structurally malformed, which should trigger a parsing error
SAMPLE_XML_CONTENT_BAD = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250909" />
  <!-- Missing closing document tag -->
"""


class MockLoader:
    """A mock loader that does nothing but allows the CLI to run."""

    def __init__(self, db_settings):
        pass

    def start_run(self, mode):
        return 1

    def end_run(self, *args, **kwargs):
        pass

    def pre_load_optimization(self, mode):
        pass

    def bulk_load_to_staging(self, intermediate_dir):
        pass

    def merge_from_staging(self, mode):
        pass

    def post_load_cleanup(self, mode):
        pass


@pytest.fixture
def mock_db_and_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Mocks the DB loader and overrides settings for the test."""
    quarantine_dir = tmp_path / "quarantine"
    test_settings = Settings(quarantine_path=str(quarantine_dir))

    monkeypatch.setattr("py_load_spl.cli.get_settings", lambda: test_settings)
    monkeypatch.setattr(
        "py_load_spl.main.get_db_loader", lambda settings: MockLoader(settings)
    )
    return test_settings


def test_full_load_quarantines_bad_xml(
    tmp_path: Path,
    mock_db_and_settings: Settings,
):
    """
    Tests that the full-load command correctly identifies a malformed XML file,
    moves it to the quarantine directory, and successfully processes the good file.
    """
    # 1. Setup: Create source directory and files
    source_dir = tmp_path / "source_xmls"
    source_dir.mkdir()

    good_file = source_dir / "good.xml"
    good_file.write_text(SAMPLE_XML_CONTENT_GOOD)

    bad_file = source_dir / "bad.xml"
    bad_file.write_text(SAMPLE_XML_CONTENT_BAD)

    quarantine_dir = Path(mock_db_and_settings.quarantine_path)

    # 2. Execute: Run the full-load command.
    # We run with text logs to make stdout assertion easier.
    result = runner.invoke(
        app, ["--log-format", "text", "full-load", "--source", str(source_dir)]
    )

    # 3. Assertions
    assert result.exit_code == 0, f"CLI command failed with output:\n{result.stdout}"

    # Check that the bad file was moved
    assert not bad_file.exists()
    assert (quarantine_dir / "bad.xml").exists()

    # Check that the good file was not moved
    assert good_file.exists()
    assert not (quarantine_dir / "good.xml").exists()

    # Check the log messages in the captured stdout
    assert "Moved corrupted file bad.xml" in result.stdout
    assert "Quarantined 1 file(s)." in result.stdout
    assert "Full load process finished successfully" in result.stdout

    # Verify the content of the quarantined file
    assert (quarantine_dir / "bad.xml").read_text() == SAMPLE_XML_CONTENT_BAD
