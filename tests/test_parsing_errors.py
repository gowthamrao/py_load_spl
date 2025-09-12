from pathlib import Path

from py_load_spl.parsing import parse_spl_file


def test_parsing_handles_missing_version_number_gracefully(tmp_path: Path) -> None:
    """
    Tests that the parser gracefully handles a missing <versionNumber> element
    by assigning a default value, rather than raising an error.
    """
    # This XML is missing the <versionNumber> element. The parsing code
    # should handle this gracefully and default the version to 0.
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="a" />
  <setId root="b" />
  <effectiveTime value="20250909" />
</document>
"""
    file_path = tmp_path / "test.xml"
    file_path.write_text(xml_content)

    # Act
    data = parse_spl_file(file_path)

    # Assert
    assert data["version_number"] == 0
