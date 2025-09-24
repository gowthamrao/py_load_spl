"""
This module defines the explicit PyArrow schemas for each of the Pydantic
data models used in the application.

Defining these schemas explicitly (FRD N002.1) ensures that the data written to
Parquet files has the correct and consistent data types, avoiding the
brittleness of type inference from data batches. This is crucial for data
integrity and reliability.
"""

import pyarrow as pa

# Schema for the 'products' table (from models.Product)
PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("set_id", pa.string(), nullable=False),
        pa.field("version_number", pa.int64(), nullable=False),
        pa.field("effective_time", pa.date32(), nullable=False),
        pa.field("product_name", pa.string()),
        pa.field("manufacturer_name", pa.string()),
        pa.field("dosage_form", pa.string()),
        pa.field("route_of_administration", pa.string()),
        pa.field("is_latest_version", pa.bool_(), nullable=False),
        pa.field("loaded_at", pa.timestamp("us"), nullable=False),
    ]
)

# Schema for the 'product_ndcs' table (from models.ProductNdc)
PRODUCT_NDC_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("ndc_code", pa.string(), nullable=False),
    ]
)

# Schema for the 'spl_raw_documents' table (from models.SplRawDocument)
SPL_RAW_DOCUMENT_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("set_id", pa.string(), nullable=False),
        pa.field("version_number", pa.int64(), nullable=False),
        pa.field("effective_time", pa.date32(), nullable=False),
        # raw_data is converted from XML to a JSON string before writing
        pa.field("raw_data", pa.string()),
        pa.field("source_filename", pa.string(), nullable=False),
        pa.field("loaded_at", pa.timestamp("us"), nullable=False),
    ]
)

# Schema for the 'ingredients' table (from models.Ingredient)
INGREDIENT_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("ingredient_name", pa.string()),
        pa.field("substance_code", pa.string()),
        pa.field("strength_numerator", pa.string()),
        pa.field("strength_denominator", pa.string()),
        pa.field("unit_of_measure", pa.string()),
        pa.field("is_active_ingredient", pa.bool_(), nullable=False),
    ]
)

# Schema for the 'packaging' table (from models.Packaging)
PACKAGING_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("package_ndc", pa.string()),
        pa.field("package_description", pa.string()),
        pa.field("package_type", pa.string()),
    ]
)

# Schema for the 'marketing_status' table (from models.MarketingStatus)
MARKETING_STATUS_SCHEMA = pa.schema(
    [
        pa.field("document_id", pa.string(), nullable=False),
        pa.field("marketing_category", pa.string()),
        pa.field("start_date", pa.date32()),
        pa.field("end_date", pa.date32()),
    ]
)
