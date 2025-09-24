from pathlib import Path

import pytest

from py_load_spl.parsing import SplParsingError, parse_spl_file


@pytest.fixture
def xxe_spl_file(tmp_path: Path) -> Path:
    """Creates a malicious SPL XML file with an XXE payload to read a file."""
    secret_file = tmp_path / "secret.txt"
    secret_content = "THIS IS A SECRET"
    secret_file.write_text(secret_content)

    xxe_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE document [
  <!ENTITY xxe SYSTEM "file://{secret_file.absolute()}">
]>
<document xmlns="urn:hl7-org:v3">
  <id root="xxe-test-doc" />
  <component>
    <structuredBody>
       <component>
          <section>
             <text>Here is the secret: &xxe;</text>
          </section>
       </component>
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / "xxe_spl.xml"
    file_path.write_text(xxe_content)
    return file_path


@pytest.fixture
def billion_laughs_spl_file(tmp_path: Path) -> Path:
    """Creates a malicious SPL XML file for a 'billion laughs' DoS attack."""
    content = """<?xml version="1.0"?>
<!DOCTYPE lolz [
 <!ENTITY lol "lol">
 <!ENTITY lol2 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
 <!ENTITY lol3 "&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;">
 <!ENTITY lol4 "&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;&lol3;">
 <!ENTITY lol5 "&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;&lol4;">
 <!ENTITY lol6 "&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;&lol5;">
 <!ENTITY lol7 "&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;&lol6;">
 <!ENTITY lol8 "&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;&lol7;">
 <!ENTITY lol9 "&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;&lol8;">
]>
<document xmlns="urn:hl7-org:v3">
  <id root="billion-laughs-doc" />
  <text>&lol9;</text>
</document>
"""
    file_path = tmp_path / "billion_laughs.xml"
    file_path.write_text(content)
    return file_path


def test_parser_is_not_vulnerable_to_xxe(xxe_spl_file: Path) -> None:
    """
    Tests that the parser does not resolve external entities, preventing
    an XXE attack that would read a local file.
    """
    # The parser with resolve_entities=False does not resolve the entity,
    # and lxml does not raise an error for the undefined entity in this context.
    # The important part is that the file content is not read.
    data = parse_spl_file(xxe_spl_file)
    assert "THIS IS A SECRET" not in data["raw_data"]
    # We can also check that the entity reference is still in the raw data,
    # proving it was not resolved.
    assert "&xxe;" in data["raw_data"]


def test_parser_is_not_vulnerable_to_billion_laughs(
    billion_laughs_spl_file: Path,
) -> None:
    """
    Tests that the parser is not vulnerable to a 'billion laughs' DoS attack.
    lxml has built-in protection against this, which should raise an error.
    """
    with pytest.raises(SplParsingError) as excinfo:
        parse_spl_file(billion_laughs_spl_file)

    assert "Maximum entity amplification factor exceeded" in str(excinfo.value)


@pytest.fixture
def quadratic_blowup_spl_file(tmp_path: Path) -> Path:
    """Creates a malicious SPL XML file for a 'quadratic blowup' DoS attack."""
    # This creates an attribute value of over 10 million characters, which should
    # trigger lxml's built-in protections against huge attributes or entity expansion.
    entity_content = "a" * 100_000
    malicious_root_value = "&a;" * 101  # Results in > 10MB string
    content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE document [
  <!ENTITY a "{entity_content}">
]>
<document xmlns="urn:hl7-org:v3">
  <id root="{malicious_root_value}" />
  <component>
    <structuredBody>
       <component>
          <section>
             <text>Some text</text>
          </section>
       </component>
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / "quadratic_blowup.xml"
    file_path.write_text(content)
    return file_path


def test_parser_is_not_vulnerable_to_quadratic_blowup(
    quadratic_blowup_spl_file: Path,
) -> None:
    """
    Tests that the parser is not vulnerable to a 'quadratic blowup' DoS attack.
    lxml has built-in protection against this, which should raise an error.
    """
    with pytest.raises(SplParsingError) as excinfo:
        parse_spl_file(quadratic_blowup_spl_file)

    # We expect lxml to raise an error due to the massive entity expansion.
    # The error message could be about a "huge attribute" or "entity amplification".
    error_str = str(excinfo.value).lower()
    assert "huge" in error_str or "amplification" in error_str
