from pathlib import Path

import pytest

from py_load_spl.parsing import SplParsingError, parse_spl_file


def test_parsing_handles_attribute_error(tmp_path: Path):
    """
    Tests that the parser's main try/except block correctly catches an
    AttributeError (e.g., from a missing attribute on an XML element)
    and wraps it in a SplParsingError.
    """
    # This XML is missing the <versionNumber> element. The parsing code
    # will call .get() on the result of finding this element, which will be
    # None, triggering an AttributeError. This should be caught.
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="a" />
  <setId root="b" />
  <effectiveTime value="20250909" />
</document>
"""
    file_path = tmp_path / "test.xml"
    file_path.write_text(xml_content)

    with pytest.raises(SplParsingError):
        parse_spl_file(file_path)
