-- DDL for py-load-spl SQLite database
-- Adapted from the PostgreSQL schema, based on FRD Section 4

PRAGMA journal_mode=WAL; -- Set journal mode for better performance
PRAGMA foreign_keys=ON; -- Enforce foreign key constraints by default

-- =================================================================
-- ETL Tracking Schema (FRD Sec 4.3)
-- =================================================================

CREATE TABLE IF NOT EXISTS etl_load_history (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time TEXT,
    end_time TEXT,
    status TEXT,
    mode TEXT,
    archives_processed INTEGER,
    records_loaded INTEGER,
    error_log TEXT
);

CREATE TABLE IF NOT EXISTS etl_processed_archives (
    archive_name TEXT PRIMARY KEY,
    archive_checksum TEXT,
    processed_timestamp TEXT
);

-- =================================================================
-- Full Representation Schema (FRD Sec 4.2)
-- =================================================================

CREATE TABLE IF NOT EXISTS spl_raw_documents (
    document_id TEXT PRIMARY KEY,
    set_id TEXT,
    version_number INTEGER,
    effective_time TEXT,
    raw_data TEXT, -- Storing as TEXT, expected to be a JSON string
    source_filename TEXT,
    loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for raw documents
CREATE TABLE IF NOT EXISTS spl_raw_documents_staging (
    document_id TEXT,
    set_id TEXT,
    version_number INTEGER,
    effective_time TEXT,
    raw_data TEXT,
    source_filename TEXT,
    loaded_at TEXT
);


-- =================================================================
-- Standard Representation Schema (FRD Sec 4.1)
-- =================================================================

-- Production Tables

CREATE TABLE IF NOT EXISTS products (
    document_id TEXT PRIMARY KEY,
    set_id TEXT,
    version_number INTEGER,
    effective_time TEXT,
    product_name TEXT,
    manufacturer_name TEXT,
    dosage_form TEXT,
    route_of_administration TEXT,
    is_latest_version INTEGER, -- Using INTEGER for BOOLEAN (0 or 1)
    loaded_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES spl_raw_documents(document_id)
);

CREATE TABLE IF NOT EXISTS product_ndcs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    ndc_code TEXT,
    FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS ingredients (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    ingredient_name TEXT,
    substance_code TEXT,
    strength_numerator TEXT,
    strength_denominator TEXT,
    unit_of_measure TEXT,
    is_active_ingredient INTEGER, -- Using INTEGER for BOOLEAN (0 or 1)
    FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS packaging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    package_ndc TEXT,
    package_description TEXT,
    package_type TEXT,
    FOREIGN KEY (document_id) REFERENCES products(document_id)
);

CREATE TABLE IF NOT EXISTS marketing_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id TEXT,
    marketing_category TEXT,
    start_date TEXT,
    end_date TEXT,
    FOREIGN KEY (document_id) REFERENCES products(document_id)
);

-- Staging Tables (for bulk loading)
-- These schemas match the CSV file structure and the columns expected by the SqliteLoader.
-- They do NOT have primary keys or foreign keys for maximum loading speed.

CREATE TABLE IF NOT EXISTS products_staging (
    document_id TEXT,
    set_id TEXT,
    version_number INTEGER,
    effective_time TEXT,
    product_name TEXT,
    manufacturer_name TEXT,
    dosage_form TEXT,
    route_of_administration TEXT,
    is_latest_version INTEGER,
    loaded_at TEXT
);

CREATE TABLE IF NOT EXISTS product_ndcs_staging (
    document_id TEXT,
    ndc_code TEXT
);

CREATE TABLE IF NOT EXISTS ingredients_staging (
    document_id TEXT,
    ingredient_name TEXT,
    substance_code TEXT,
    strength_numerator TEXT,
    strength_denominator TEXT,
    unit_of_measure TEXT,
    is_active_ingredient INTEGER
);

CREATE TABLE IF NOT EXISTS packaging_staging (
    document_id TEXT,
    package_ndc TEXT,
    package_description TEXT,
    package_type TEXT
);

CREATE TABLE IF NOT EXISTS marketing_status_staging (
    document_id TEXT,
    marketing_category TEXT,
    start_date TEXT,
    end_date TEXT
);


-- =================================================================
-- Indexes for Query Performance
-- =================================================================

CREATE INDEX IF NOT EXISTS idx_products_versioning ON products (set_id, version_number DESC, effective_time DESC);
CREATE INDEX IF NOT EXISTS idx_products_is_latest ON products (is_latest_version);
CREATE INDEX IF NOT EXISTS idx_product_ndcs_ndc_code ON product_ndcs (ndc_code);
CREATE INDEX IF NOT EXISTS idx_ingredients_substance_code ON ingredients (substance_code);
CREATE INDEX IF NOT EXISTS idx_packaging_package_ndc ON packaging (package_ndc);
CREATE INDEX IF NOT EXISTS idx_spl_raw_documents_set_id ON spl_raw_documents (set_id);
