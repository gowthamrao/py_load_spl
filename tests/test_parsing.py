from pathlib import Path

import pytest

from py_load_spl.parsing import SplParsingError, parse_spl_file


@pytest.fixture
def sample_spl_file(tmp_path: Path) -> Path:
    """Create a temporary sample SPL XML file for testing."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
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
        <asEntityWithGeneric>
          <genericMedicine>
            <name>SAMPLAMYCIN</name>
          </genericMedicine>
        </asEntityWithGeneric>
        <asEquivalentEntity>
            <code code="12345-678" codeSystem="2.16.840.1.113883.6.69" />
        </asEquivalentEntity>
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
    file_path = tmp_path / "sample_spl.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parse_spl_file(sample_spl_file: Path) -> None:
    """
    Tests the happy path for parsing a well-formed SPL file.
    """
    data = parse_spl_file(sample_spl_file)

    # Assert metadata
    assert data["document_id"] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
    assert data["set_id"] == "a2c3b6f0-a38f-4b48-96eb-3b2b403816a4"
    assert data["version_number"] == 1
    assert data["effective_time"] == "20250907"

    # Assert product details
    assert data["product_name"] == "Sample Drug"
    assert data["manufacturer_name"] == "Sample Pharmaceuticals"
    assert data["dosage_form"] == "TABLET"

    # Assert ingredients
    assert len(data["ingredients"]) == 1
    ingredient = data["ingredients"][0]
    assert ingredient["ingredient_name"] == "SAMPLESTAT"
    assert ingredient["substance_code"] == "UNII-SAMPLE"
    assert ingredient["is_active_ingredient"] is True
    assert ingredient["strength_numerator"] == "100"
    assert ingredient["strength_denominator"] == "1"
    assert ingredient["unit_of_measure"] == "mg"

    # Assert packaging
    assert len(data["packaging"]) == 1
    package = data["packaging"][0]
    assert package["package_ndc"] == "12345-678-90"
    assert package["package_description"] == "30 Tablets in 1 Bottle"
    assert package["package_type"] == "BOTTLE"

    # Assert product NDCs
    assert len(data["product_ndcs"]) == 1
    ndc = data["product_ndcs"][0]
    assert ndc["ndc_code"] == "12345-678"

    # Assert marketing status
    assert len(data["marketing_status"]) == 1
    status = data["marketing_status"][0]
    assert status["marketing_category"] == "active"
    assert status["start_date"] == "20250101"


@pytest.fixture
def spl_file_with_multiple_statuses(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with multiple marketing statuses."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="d1b64b62-050a-4895-924c-d2862d2a6a69" />
  <setId root="a2c3b6f0-a38f-4b48-96eb-3b2b403816a4" />
  <versionNumber value="1" />
  <effectiveTime value="20250907" />
  <component>
    <structuredBody>
      <component>
        <section>
          <subject>
            <marketingAct>
              <statusCode code="completed"/>
              <effectiveTime>
                <low value="20240101"/>
                <high value="20241231"/>
              </effectiveTime>
            </marketingAct>
          </subject>
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
    file_path = tmp_path / "multi_status.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_multiple_marketing_statuses(
    spl_file_with_multiple_statuses: Path,
) -> None:
    """
    Tests that the parser correctly extracts multiple marketing status entries,
    including those with and without an end date.
    """
    parsed_data = parse_spl_file(spl_file_with_multiple_statuses)

    assert "marketing_status" in parsed_data
    assert isinstance(parsed_data["marketing_status"], list)
    assert len(parsed_data["marketing_status"]) == 2

    # Sort by start_date for predictable order
    statuses = sorted(
        parsed_data["marketing_status"], key=lambda x: x.get("start_date")
    )

    assert statuses[0]["marketing_category"] == "completed"
    assert statuses[0]["start_date"] == "20240101"
    assert statuses[0]["end_date"] == "20241231"

    assert statuses[1]["marketing_category"] == "active"
    assert statuses[1]["start_date"] == "20250101"
    assert statuses[1]["end_date"] is None


def test_parse_spl_file_empty_file(tmp_path: Path) -> None:
    """Tests that parsing an empty file raises SplParsingError."""
    file_path = tmp_path / "empty.xml"
    file_path.write_text("")
    with pytest.raises(SplParsingError):
        parse_spl_file(file_path)


def test_parse_spl_file_invalid_xml(tmp_path: Path) -> None:
    """Tests that parsing a non-xml file raises SplParsingError."""
    file_path = tmp_path / "invalid.xml"
    file_path.write_text("this is not xml")
    with pytest.raises(SplParsingError):
        parse_spl_file(file_path)


def test_parse_spl_file_no_document_tag(tmp_path: Path) -> None:
    """Tests that parsing a file without a <document> tag raises SplParsingError."""
    file_path = tmp_path / "no_doc_tag.xml"
    file_path.write_text("<root><item>1</item></root>")
    with pytest.raises(SplParsingError):
        parse_spl_file(file_path)


@pytest.fixture
def spl_file_with_multiple_ingredients(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with multiple ingredients."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="multi-ingr-doc" />
  <setId root="multi-ingr-set" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <ingredient classCode="ACT">
          <ingredientSubstance><name>INGREDIENT-A</name></ingredientSubstance>
        </ingredient>
        <ingredient classCode="INA">
          <ingredientSubstance><name>INGREDIENT-B</name></ingredientSubstance>
        </ingredient>
      </manufacturedProduct>
    </manufacturedProduct>
  </subject>
</document>
"""
    file_path = tmp_path / "multi_ingredient.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_multiple_ingredients(spl_file_with_multiple_ingredients: Path) -> None:
    """Tests that the parser correctly extracts multiple ingredients."""
    data = parse_spl_file(spl_file_with_multiple_ingredients)
    assert len(data["ingredients"]) == 2
    assert data["ingredients"][0]["ingredient_name"] == "INGREDIENT-A"
    assert data["ingredients"][0]["is_active_ingredient"] is True
    assert data["ingredients"][1]["ingredient_name"] == "INGREDIENT-B"
    assert data["ingredients"][1]["is_active_ingredient"] is False


@pytest.fixture
def spl_file_with_multiple_packages(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with multiple packaging entries."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="multi-pkg-doc" />
  <setId root="multi-pkg-set" />
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="34069-5" displayName="PACKAGE LABEL" />
          <component><section>
            <part>
              <code code="NDC-1" />
              <name>10 Vials per Carton</name>
            </part>
            <part>
              <code code="NDC-2" />
              <name>1 Vial</name>
            </part>
          </section></component>
        </section>
      </component>
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / "multi_package.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_multiple_packaging_entries(
    spl_file_with_multiple_packages: Path,
) -> None:
    """Tests that the parser correctly extracts multiple packaging entries."""
    data = parse_spl_file(spl_file_with_multiple_packages)
    assert len(data["packaging"]) == 2
    assert data["packaging"][0]["package_ndc"] == "NDC-1"
    assert data["packaging"][1]["package_ndc"] == "NDC-2"


@pytest.fixture
def spl_file_missing_attribute(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with a missing 'root' attribute."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id />
  <setId root="missing-attr-set" />
</document>
"""
    file_path = tmp_path / "missing_attribute.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_missing_attribute(spl_file_missing_attribute: Path) -> None:
    """Tests parsing of an element with a missing attribute."""
    data = parse_spl_file(spl_file_missing_attribute)
    assert data["document_id"] is None
    assert data["set_id"] == "missing-attr-set"


@pytest.fixture
def spl_file_no_ingredients(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with no ingredients."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="no-ingr-doc" />
  <subject>
    <manufacturedProduct>
      <manufacturedProduct>
        <name>Drug With No Ingredients Listed</name>
      </manufacturedProduct>
    </manufacturedProduct>
  </subject>
</document>
"""
    file_path = tmp_path / "no_ingredients.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_no_ingredients(spl_file_no_ingredients: Path) -> None:
    """Tests that a file with no ingredients is parsed without error."""
    data = parse_spl_file(spl_file_no_ingredients)
    assert data["ingredients"] == []


@pytest.fixture
def spl_file_no_packaging(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with no packaging section."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="no-pkg-doc" />
  <component>
    <structuredBody>
      <!-- No packaging section here -->
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / "no_packaging.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_no_packaging(spl_file_no_packaging: Path) -> None:
    """Tests that a file with no packaging is parsed without error."""
    data = parse_spl_file(spl_file_no_packaging)
    assert data["packaging"] == []


@pytest.fixture
def spl_file_with_package_desc_tag(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file where packaging uses <desc> instead of <name>."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="pkg-desc-fallback-doc" />
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="34069-5" displayName="PACKAGE LABEL" />
          <component><section>
            <part>
              <code code="NDC-DESC" />
              <desc>Description from desc tag</desc>
            </part>
          </section></component>
        </section>
      </component>
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / "pkg_desc_fallback.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_packaging_with_desc_tag(spl_file_with_package_desc_tag: Path) -> None:
    """Tests that the parser correctly falls back to the <desc> tag for packaging description."""
    data = parse_spl_file(spl_file_with_package_desc_tag)
    assert len(data["packaging"]) == 1
    assert data["packaging"][0]["package_ndc"] == "NDC-DESC"
    assert data["packaging"][0]["package_description"] == "Description from desc tag"


@pytest.mark.parametrize("packaging_code", ["34069-5", "51945-4"])
def test_parsing_packaging_section_codes(tmp_path: Path, packaging_code: str) -> None:
    """Tests that both recognized packaging section codes are parsed correctly."""
    spl_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="pkg-code-test-doc" />
  <component>
    <structuredBody>
      <component>
        <section>
          <code code="{packaging_code}" displayName="PACKAGE LABEL" />
          <component><section>
            <part>
              <code code="NDC-PARAM-TEST" />
              <name>Package from code {packaging_code}</name>
            </part>
          </section></component>
        </section>
      </component>
    </structuredBody>
  </component>
</document>
"""
    file_path = tmp_path / f"pkg_code_{packaging_code}.xml"
    file_path.write_text(spl_content)

    data = parse_spl_file(file_path)

    assert len(data["packaging"]) == 1
    assert data["packaging"][0]["package_ndc"] == "NDC-PARAM-TEST"
    assert (
        data["packaging"][0]["package_description"]
        == f"Package from code {packaging_code}"
    )


@pytest.fixture
def spl_file_invalid_version(tmp_path: Path) -> Path:
    """Creates a temporary SPL XML file with a non-integer version number."""
    spl_content = """<?xml version="1.0" encoding="UTF-8"?>
<document xmlns="urn:hl7-org:v3">
  <id root="invalid-version-doc" />
  <versionNumber value="not-a-number" />
</document>
"""
    file_path = tmp_path / "invalid_version.xml"
    file_path.write_text(spl_content)
    return file_path


def test_parsing_invalid_version_number(spl_file_invalid_version: Path) -> None:
    """Tests that a non-integer version number raises a SplParsingError."""
    with pytest.raises(SplParsingError) as excinfo:
        parse_spl_file(spl_file_invalid_version)
    assert "A critical error occurred during parsing" in str(excinfo.value)


def test_parse_real_sample_spl_file() -> None:
    """
    Tests parsing a real, more complex sample SPL file from the project root.
    """
    # The file is in the root, so we go up one level from the tests directory
    real_sample_file = Path(__file__).parent.parent / "sample_spl.xml"
    assert real_sample_file.exists(), f"Sample file not found at {real_sample_file}"

    data = parse_spl_file(real_sample_file)

    # Assert metadata from the sample file
    assert data["document_id"] == "d1b64b62-050a-4895-924c-d2862d2a6a69"
    assert data["set_id"] == "a2c3b6f0-a38f-4b48-96eb-3b2b403816a4"
    assert data["version_number"] == 1
    assert data["effective_time"] == "20250907"

    # Assert product details
    assert data["product_name"] == "Jules's Sample Drug"
    assert data["manufacturer_name"] == "Jules Pharmaceuticals"
    assert data["dosage_form"] == "TABLET"

    # Assert ingredients
    assert len(data["ingredients"]) == 1
    ingredient = data["ingredients"][0]
    assert ingredient["ingredient_name"] == "JULESTAT"
    assert ingredient["substance_code"] == "UNII-JULE"
    assert ingredient["is_active_ingredient"] is True
    assert ingredient["strength_numerator"] == "100"
    assert ingredient["strength_denominator"] == "1"
    assert ingredient["unit_of_measure"] == "mg"

    # Assert packaging
    assert len(data["packaging"]) == 1
    package = data["packaging"][0]
    assert package["package_ndc"] == "12345-678-90"
    assert package["package_description"] == "30 Tablets in 1 Bottle"
    assert package["package_type"] == "BOTTLE"

    # Assert product NDCs
    assert len(data["product_ndcs"]) == 1
    ndc = data["product_ndcs"][0]
    assert ndc["ndc_code"] == "12345-678"

    # Assert marketing status
    assert len(data["marketing_status"]) == 1
    status = data["marketing_status"][0]
    assert status["marketing_category"] == "active"
    assert status["start_date"] == "20250101"
