from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


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
    is_latest_version: bool = Field(default=True)
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("effective_time", mode="before")
    @classmethod
    def parse_effective_time(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d").date()
        return v


class ProductNdc(BaseModel):
    """Data model for the 'product_ndcs' table."""

    document_id: UUID
    ndc_code: str


class SplRawDocument(BaseModel):
    """Data model for the 'spl_raw_documents' table (Full Representation)."""

    document_id: UUID
    set_id: UUID
    version_number: int
    effective_time: date
    raw_data: str
    source_filename: str
    loaded_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("effective_time", mode="before")
    @classmethod
    def parse_effective_time(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d").date()
        return v


class Ingredient(BaseModel):
    """Data model for the 'ingredients' table."""

    document_id: UUID
    ingredient_name: str | None = Field(default=None)
    substance_code: str | None = Field(default=None)
    strength_numerator: str | None = Field(default=None)
    strength_denominator: str | None = Field(default=None)
    unit_of_measure: str | None = Field(default=None)
    is_active_ingredient: bool


class Packaging(BaseModel):
    """Data model for the 'packaging' table."""

    document_id: UUID
    package_ndc: str | None = Field(default=None)
    package_description: str | None = Field(default=None)
    package_type: str | None = Field(default=None)


class MarketingStatus(BaseModel):
    """Data model for the 'marketing_status' table."""

    document_id: UUID
    marketing_category: str | None = Field(default=None)
    start_date: date | None = Field(default=None)
    end_date: date | None = Field(default=None)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date | None:
        """Parse date from 'YYYYMMDD' string format."""
        if isinstance(v, str):
            return datetime.strptime(v, "%Y%m%d").date()
        return v
