from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

# --- Reusable Validators (F003.3) ---


def clean_string(v: str | None) -> str | None:
    """
    Reusable Pydantic validator to clean string fields.
    - Strips leading/trailing whitespace.
    - Converts empty strings to None for database consistency.
    """
    if isinstance(v, str):
        stripped = v.strip()
        return stripped if stripped else None
    return v


# --- Data Models (FRD Section 4) ---


class Product(BaseModel):
    """Data model for the 'products' table, based on FRD Section 4.1."""

    document_id: UUID
    set_id: UUID
    version_number: int
    effective_time: date
    product_name: str | None = Field(default=None)
    manufacturer_name: str | None = Field(default=None)
    dosage_form: str | None = Field(default=None)
    route_of_administration: str | None = Field(default=None)
    is_latest_version: bool = Field(default=False)
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    _clean_strings = field_validator(
        "product_name",
        "manufacturer_name",
        "dosage_form",
        "route_of_administration",
        mode="before",
    )(clean_string)

    @field_validator("effective_time", mode="before")
    @classmethod
    def parse_effective_time(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d").date()
        if isinstance(v, date) or v is None:
            return v
        raise TypeError("Unsupported type for effective_time")


class Archive(BaseModel):
    """Data model for a downloadable SPL archive file."""

    name: str
    url: str
    checksum: str

    _clean_strings = field_validator("name", "url", "checksum", mode="before")(
        clean_string
    )


class ProductNdc(BaseModel):
    """Data model for the 'product_ndcs' table."""

    document_id: UUID
    ndc_code: str

    _clean_strings = field_validator("ndc_code", mode="before")(clean_string)


class SplRawDocument(BaseModel):
    """Data model for the 'spl_raw_documents' table (Full Representation)."""

    document_id: UUID
    set_id: UUID
    version_number: int
    effective_time: date
    raw_data: str | None  # Raw XML/JSON, should not be cleaned
    source_filename: str
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    _clean_source_filename = field_validator("source_filename", mode="before")(
        clean_string
    )

    @field_validator("effective_time", mode="before")
    @classmethod
    def parse_effective_time(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d").date()
        if isinstance(v, date) or v is None:
            return v
        raise TypeError("Unsupported type for effective_time")


class Ingredient(BaseModel):
    """Data model for the 'ingredients' table."""

    document_id: UUID
    ingredient_name: str | None = Field(default=None)
    substance_code: str | None = Field(default=None)
    strength_numerator: str | None = Field(default=None)
    strength_denominator: str | None = Field(default=None)
    unit_of_measure: str | None = Field(default=None)
    is_active_ingredient: bool

    _clean_strings = field_validator(
        "ingredient_name",
        "substance_code",
        "strength_numerator",
        "strength_denominator",
        "unit_of_measure",
        mode="before",
    )(clean_string)


class Packaging(BaseModel):
    """Data model for the 'packaging' table."""

    document_id: UUID
    package_ndc: str | None = Field(default=None)
    package_description: str | None = Field(default=None)
    package_type: str | None = Field(default=None)

    _clean_strings = field_validator(
        "package_ndc", "package_description", "package_type", mode="before"
    )(clean_string)


class MarketingStatus(BaseModel):
    """Data model for the 'marketing_status' table."""

    document_id: UUID
    marketing_category: str | None = Field(default=None)
    start_date: date | None = Field(default=None)
    end_date: date | None = Field(default=None)

    _clean_strings = field_validator("marketing_category", mode="before")(clean_string)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            # Handle empty strings gracefully, returning None
            if not v.strip():
                return None
            return datetime.strptime(v, "%Y%m%d").date()
        if isinstance(v, date) or v is None:
            return v
        raise TypeError("Unsupported type for date")
