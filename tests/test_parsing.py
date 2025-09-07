import pytest
from pathlib import Path

from py_load_spl.parsing import parse_spl_file


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
        <name>Jules's Sample Drug</name>
        <formCode code="C42916" displayName="TABLET" />
        <asEntityWithGeneric>
          <genericMedicine>
            <name>JULAMYCIN</name>
          </genericMedicine>
        </asEntityWithGeneric>
        <ingredient classCode="ACT">
          <quantity>
            <numerator value="100" unit="mg" />
            <denominator value="1" unit="TABLET" />
          </quantity>
          <ingredientSubstance>
            <name>JULESTAT</name>
            <code code="UNII-JULE" displayName="JULESTAT" />
          </ingredientSubstance>
        </ingredient>
      </manufacturedProduct>
      <manufacturer>
        <name>Jules Pharmaceuticals</name>
      </manufacturer>
    </manufacturedProduct>
  </subject>
  <component>
    <structuredBody>
      <component>
        <section ID="s2">
          <code code="51945-4" displayName="PACKAGE LABEL.PRINCIPAL DISPLAY PANEL" />
          <text>
            NDC 12345-678-90
          </text>
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


def test_parse_spl_file(sample_spl_file: Path):
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
    assert package["package_ndc"] == "NDC 12345-678-90"

    # Assert marketing status
    assert len(data["marketing_status"]) == 1
    status = data["marketing_status"][0]
    assert status["marketing_category"] == "active"
    assert status["start_date"] == "20250101"
