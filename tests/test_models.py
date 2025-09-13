import uuid
from datetime import date

import pytest

from py_load_spl.models import (
    Ingredient,
    MarketingStatus,
    Packaging,
    Product,
    ProductNdc,
    clean_string,
)


@pytest.mark.parametrize(
    "input_val, expected_val",
    [
        ("  some string  ", "some string"),
        ("\tanother string\n", "another string"),
        ("no extra space", "no extra space"),
        ("   ", None),  # String with only spaces should become None
        ("", None),  # Empty string should become None
        (None, None),  # None should remain None
    ],
)
def test_clean_string_validator(
    input_val: str | None, expected_val: str | None
) -> None:
    """
    Tests the reusable clean_string validator directly.
    """
    assert clean_string(input_val) == expected_val


def test_product_model_cleaning() -> None:
    """
    Tests that the string cleaning validator is correctly applied to the Product model.
    """
    p = Product(
        document_id=uuid.uuid4(),
        set_id=uuid.uuid4(),
        version_number=1,
        effective_time=date(2024, 1, 1),
        product_name="  Test Product  ",
        manufacturer_name="   ",  # Should be None
        dosage_form="\tDosage  ",
        route_of_administration="",  # Should be None
    )
    assert p.product_name == "Test Product"
    assert p.manufacturer_name is None
    assert p.dosage_form == "Dosage"
    assert p.route_of_administration is None


def test_ingredient_model_cleaning() -> None:
    """
    Tests that the string cleaning validator is correctly applied to the Ingredient model.
    """
    i = Ingredient(
        document_id=uuid.uuid4(),
        ingredient_name="  Active Ingredient ",
        substance_code="  CODE123",
        strength_numerator=" 50 ",
        strength_denominator=" 100 ",
        unit_of_measure=" mg ",
        is_active_ingredient=True,
    )
    assert i.ingredient_name == "Active Ingredient"
    assert i.substance_code == "CODE123"
    assert i.strength_numerator == "50"
    assert i.strength_denominator == "100"
    assert i.unit_of_measure == "mg"


def test_packaging_model_cleaning() -> None:
    """
    Tests that the string cleaning validator is correctly applied to the Packaging model.
    """
    p = Packaging(
        document_id=uuid.uuid4(),
        package_ndc="  NDC-123  ",
        package_description="  Box of 100 ",
        package_type="  ",
    )
    assert p.package_ndc == "NDC-123"
    assert p.package_description == "Box of 100"
    assert p.package_type is None


def test_marketing_status_model_cleaning() -> None:
    """
    Tests that the string cleaning validator is correctly applied to the MarketingStatus model.
    """
    m = MarketingStatus(
        document_id=uuid.uuid4(),
        marketing_category="  Active  ",
        start_date=date(2024, 1, 1),
    )
    assert m.marketing_category == "Active"


def test_product_ndc_model_cleaning() -> None:
    """
    Tests that the string cleaning validator is correctly applied to the ProductNdc model.
    """
    ndc = ProductNdc(document_id=uuid.uuid4(), ndc_code="  12345-678-90  ")
    assert ndc.ndc_code == "12345-678-90"
