-- DDL for py-load-spl PostgreSQL database
-- Based on FRD Section 4

-- =================================================================
-- ETL Tracking Schema (FRD Sec 4.3)
-- =================================================================

CREATE TABLE IF NOT EXISTS etl_load_history (
    run_id BIGSERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    status VARCHAR(20),
    mode VARCHAR(20),
    archives_processed INT,
    records_loaded BIGINT,
    error_log TEXT
);

CREATE TABLE IF NOT EXISTS etl_processed_archives (
    archive_name TEXT PRIMARY KEY,
    archive_checksum VARCHAR(128),
    processed_timestamp TIMESTAMPTZ
);

-- =================================================================
-- Full Representation Schema (FRD Sec 4.2)
-- =================================================================

CREATE TABLE IF NOT EXISTS spl_raw_documents (
    document_id UUID PRIMARY KEY,
    set_id UUID,
    version_number INT,
    effective_time DATE,
    raw_data JSONB,
    source_filename TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spl_raw_documents_staging (
    document_id UUID PRIMARY KEY,
    set_id UUID,
    version_number INT,
    effective_time DATE,
    raw_data JSONB,
    source_filename TEXT,
    loaded_at TIMESTAMPTZ DEFAULT now()
);


-- =================================================================
-- Standard Representation Schema (FRD Sec 4.1)
-- =================================================================

-- Production Tables

CREATE TABLE IF NOT EXISTS products (
    document_id UUID PRIMARY KEY,
    set_id UUID,
    version_number INT,
    effective_time DATE,
    product_name TEXT,
    manufacturer_name TEXT,
    dosage_form TEXT,
    route_of_administration TEXT,
    is_latest_version BOOLEAN,
    loaded_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT fk_document_id FOREIGN KEY (document_id) REFERENCES spl_raw_documents(document_id)
);

CREATE TABLE IF NOT EXISTS product_ndcs (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID,
    ndc_code TEXT,
    CONSTRAINT fk_document_id FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS ingredients (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID,
    ingredient_name TEXT,
    substance_code TEXT,
    strength_numerator TEXT,
    strength_denominator TEXT,
    unit_of_measure TEXT,
    is_active_ingredient BOOLEAN,
    CONSTRAINT fk_document_id FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS packaging (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID,
    package_ndc TEXT,
    package_description TEXT,
    package_type TEXT,
    CONSTRAINT fk_document_id FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS marketing_status (
    id BIGSERIAL PRIMARY KEY,
    document_id UUID,
    marketing_category TEXT,
    start_date DATE,
    end_date DATE,
    CONSTRAINT fk_document_id FOREIGN KEY (document_id) REFERENCES products(document_id)
);

-- Staging Tables (for bulk loading)

CREATE TABLE IF NOT EXISTS products_staging (LIKE products INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS product_ndcs_staging (LIKE product_ndcs INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS ingredients_staging (LIKE ingredients INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS packaging_staging (LIKE packaging INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS marketing_status_staging (LIKE marketing_status INCLUDING DEFAULTS);
