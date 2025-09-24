-- DDL for py-load-spl Amazon Redshift database
-- Based on the PostgreSQL schema and adapted for Redshift specifics.

-- =================================================================
-- ETL Tracking Schema
-- =================================================================

CREATE TABLE IF NOT EXISTS etl_load_history (
    run_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    mode VARCHAR(20),
    archives_processed INT,
    records_loaded BIGINT,
    error_log VARCHAR(MAX)
);

CREATE TABLE IF NOT EXISTS etl_processed_archives (
    archive_name VARCHAR(255) PRIMARY KEY,
    archive_checksum VARCHAR(128),
    processed_timestamp TIMESTAMP
);

-- =================================================================
-- Full Representation Schema
-- =================================================================

CREATE TABLE IF NOT EXISTS spl_raw_documents (
    document_id VARCHAR(36) PRIMARY KEY,
    set_id VARCHAR(36),
    version_number INT,
    effective_time DATE,
    raw_data SUPER,
    source_filename VARCHAR(MAX),
    loaded_at TIMESTAMP DEFAULT GETDATE()
);

-- Staging table for raw documents
CREATE TABLE IF NOT EXISTS spl_raw_documents_staging (
    document_id VARCHAR(36),
    set_id VARCHAR(36),
    version_number INT,
    effective_time DATE,
    raw_data SUPER,
    source_filename VARCHAR(MAX),
    loaded_at TIMESTAMP
);


-- =================================================================
-- Standard Representation Schema
-- =================================================================

-- Production Tables

CREATE TABLE IF NOT EXISTS products (
    document_id VARCHAR(36) PRIMARY KEY,
    set_id VARCHAR(36),
    version_number INT,
    effective_time DATE,
    product_name VARCHAR(MAX),
    manufacturer_name VARCHAR(MAX),
    dosage_form VARCHAR(MAX),
    route_of_administration VARCHAR(MAX),
    is_latest_version BOOLEAN,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    CONSTRAINT fk_products_spl_raw_documents FOREIGN KEY (document_id) REFERENCES spl_raw_documents(document_id)
);

CREATE TABLE IF NOT EXISTS product_ndcs (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    document_id VARCHAR(36),
    ndc_code VARCHAR(255),
    CONSTRAINT fk_product_ndcs_products FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS ingredients (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    document_id VARCHAR(36),
    ingredient_name VARCHAR(MAX),
    substance_code VARCHAR(255),
    strength_numerator VARCHAR(255),
    strength_denominator VARCHAR(255),
    unit_of_measure VARCHAR(255),
    is_active_ingredient BOOLEAN,
    CONSTRAINT fk_ingredients_products FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS packaging (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    document_id VARCHAR(36),
    package_ndc VARCHAR(255),
    package_description VARCHAR(MAX),
    package_type VARCHAR(255),
    CONSTRAINT fk_packaging_products FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS marketing_status (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    document_id VARCHAR(36),
    marketing_category VARCHAR(255),
    start_date DATE,
    end_date DATE,
    CONSTRAINT fk_marketing_status_products FOREIGN KEY (document_id) REFERENCES products(document_id)
);

-- Staging Tables (Explicitly defined for Redshift)

CREATE TABLE IF NOT EXISTS products_staging (
    document_id VARCHAR(36),
    set_id VARCHAR(36),
    version_number INT,
    effective_time DATE,
    product_name VARCHAR(MAX),
    manufacturer_name VARCHAR(MAX),
    dosage_form VARCHAR(MAX),
    route_of_administration VARCHAR(MAX),
    is_latest_version BOOLEAN,
    loaded_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_ndcs_staging (
    document_id VARCHAR(36),
    ndc_code VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS ingredients_staging (
    document_id VARCHAR(36),
    ingredient_name VARCHAR(MAX),
    substance_code VARCHAR(255),
    strength_numerator VARCHAR(255),
    strength_denominator VARCHAR(255),
    unit_of_measure VARCHAR(255),
    is_active_ingredient BOOLEAN
);

CREATE TABLE IF NOT EXISTS packaging_staging (
    document_id VARCHAR(36),
    package_ndc VARCHAR(255),
    package_description VARCHAR(MAX),
    package_type VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS marketing_status_staging (
    document_id VARCHAR(36),
    marketing_category VARCHAR(255),
    start_date DATE,
    end_date DATE
);

-- =================================================================
-- Indexes for Query Performance (Redshift manages distribution and sort keys)
-- Note: Redshift automatically creates indexes for PRIMARY KEY constraints.
-- Additional indexes can be created, but DISTSTYLE and SORTKEY are more important.
-- For simplicity, we will mirror the postgres indexes.
-- =================================================================

CREATE INDEX IF NOT EXISTS idx_products_versioning ON products (set_id, version_number DESC, effective_time DESC);
CREATE INDEX IF NOT EXISTS idx_products_is_latest ON products (is_latest_version);
CREATE INDEX IF NOT EXISTS idx_product_ndcs_ndc_code ON product_ndcs (ndc_code);
CREATE INDEX IF NOT EXISTS idx_ingredients_substance_code ON ingredients (substance_code);
CREATE INDEX IF NOT EXISTS idx_packaging_package_ndc ON packaging (package_ndc);
CREATE INDEX IF NOT EXISTS idx_spl_raw_documents_set_id ON spl_raw_documents (set_id);
