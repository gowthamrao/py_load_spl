import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from py_load_spl.config import get_settings
from py_load_spl.main import run_full_load


@pytest.fixture
def corrupt_spl_file(tmp_path: Path) -> Path:
    """Creates a corrupt SPL XML file (missing closing tag)."""
    content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="corrupt-doc" />
  <component>
    <structuredBody>
       <component>
          <section>
             <text>This is a corrupt file with a missing closing tag.</text>
          </section>
       </component>
    </structuredBody>
  </component>
<!-- Missing closing </document> tag -->
"""
    file_path = tmp_path / "corrupt_spl.xml"
    file_path.write_text(content)
    return file_path


@patch("py_load_spl.main.get_db_loader")
def test_full_load_with_corrupt_file(
    mock_get_db_loader: MagicMock, tmp_path: Path, corrupt_spl_file: Path
) -> None:
    """
    Tests that the full-load process handles a corrupt XML file gracefully,
    quarantines it, and continues processing other valid files.
    """
    # Create a directory for the source XML files
    source_dir = tmp_path / "source_xmls"
    source_dir.mkdir()

    # Create a valid SPL file to ensure the pipeline continues
    doc_id = str(uuid.uuid4())
    set_id = str(uuid.uuid4())
    effective_time = datetime.now().strftime("%Y%m%d")
    valid_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="{doc_id}" />
  <setId root="{set_id}" />
  <versionNumber value="1" />
  <effectiveTime value="{effective_time}" />
</document>
"""
    (source_dir / "valid_spl.xml").write_text(valid_content)

    # Move the corrupt file into the source directory
    corrupt_spl_file.rename(source_dir / corrupt_spl_file.name)

    # Get settings and override paths for the test
    settings = get_settings()
    settings.quarantine_path = str(tmp_path / "quarantine")
    settings.db.adapter = "sqlite"  # Use a mock-friendly adapter

    # Mock the database loader
    mock_loader = MagicMock()
    # The transformer will produce 2 records (Product, SplRawDocument) from the valid file.
    # Mock bulk_load_to_staging to return 2 to pass the integrity check.
    mock_loader.bulk_load_to_staging.return_value = 2
    mock_get_db_loader.return_value = mock_loader

    # Run the full_load function
    run_full_load(settings, source_dir)

    # Assert that the corrupt file was moved to the quarantine directory
    quarantined_file = Path(settings.quarantine_path) / "corrupt_spl.xml"
    assert quarantined_file.is_file()

    # Assert that the valid file was processed
    mock_loader.bulk_load_to_staging.assert_called_once()

    # Assert that the run was started and ended successfully
    mock_loader.start_run.assert_called_once_with(mode="full-load")
    mock_loader.end_run.assert_called_once()

    # Check the args of end_run to ensure it was a success
    end_run_args = mock_loader.end_run.call_args[0]
    assert end_run_args[1] == "SUCCESS"
