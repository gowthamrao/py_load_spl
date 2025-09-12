from pathlib import Path

import pytest
from lxml import etree

from py_load_spl.parsing import SplParsingError, parse_spl_file

# A large-ish XML content without the correct namespace
XML_CONTENT_NO_NAMESPACE = """
<document>
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <data>{}</data>
</document>
"""

# A large-ish XML content with the wrong root tag
XML_CONTENT_WRONG_TAG = """
<doc xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <data>{}</data>
</doc>
"""


@pytest.fixture
def large_xml_file(tmp_path: Path) -> Path:
    """Creates a large XML file for memory testing."""
    file_path = tmp_path / "large_test.xml"
    # Create a reasonably large file (e.g., 10MB)
    large_data = "A" * (10 * 1024 * 1024)  # 10 MB of dummy data
    file_path.write_text(XML_CONTENT_NO_NAMESPACE.format(large_data))
    return file_path


@pytest.fixture
def large_xml_file_wrong_tag(tmp_path: Path) -> Path:
    """Creates a large XML file with the wrong root tag."""
    file_path = tmp_path / "large_test_wrong_tag.xml"
    large_data = "A" * (10 * 1024 * 1024)  # 10 MB of dummy data
    file_path.write_text(XML_CONTENT_WRONG_TAG.format(large_data))
    return file_path


def test_parsing_fails_on_large_text_node(large_xml_file: Path) -> None:
    """
    Verify that lxml's built-in resource limits prevent huge text nodes
    from being loaded, and our parser handles the resulting error.
    """
    with pytest.raises(etree.XMLSyntaxError, match="Resource limit exceeded"):
        parse_spl_file(large_xml_file)


def test_parsing_fails_gracefully_on_wrong_tag(tmp_path: Path) -> None:
    """
    Verify that parsing a file with the wrong root tag (but otherwise well-formed)
    fails with our custom SplParsingError. This tests the StopIteration catch.
    """
    file_path = tmp_path / "wrong_tag.xml"
    file_path.write_text('<doc xmlns="urn:hl7-org:v3"><data/></doc>')
    with pytest.raises(
        SplParsingError, match="Could not find the root <document> element"
    ):
        parse_spl_file(file_path)


def test_parsing_fails_gracefully_on_missing_namespace(tmp_path: Path) -> None:
    """
    Verify that parsing a file with the correct tag but no namespace
    fails with our custom SplParsingError.
    """
    file_path = tmp_path / "no_namespace.xml"
    file_path.write_text("<document><data/></document>")
    with pytest.raises(
        SplParsingError, match="Could not find the root <document> element"
    ):
        parse_spl_file(file_path)
