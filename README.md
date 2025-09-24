# py-load-spl: FDA Structured Product Labeling (SPL) Data Loader

`py-load-spl` is a robust, high-performance, and extensible open-source Python package for extracting, transforming, and loading FDA Structured Product Labeling (SPL) data into relational databases.

It provides a complete ETL pipeline to make this critical public health data accessible and queryable in a standardized relational format, while also storing the complete original XML for auditing and research.

## Key Features

- **Efficient ETL Pipeline:** Downloads, parses, transforms, and loads the entire FDA SPL dataset.
- **Memory-Conscious Parsing:** Uses iterative XML parsing (`lxml.iterparse`) to handle large files with a low, constant memory footprint.
- **Normalized Relational Schema:** Transforms complex, hierarchical XML into a clean, queryable, and normalized database schema.
- **Full Data Representation:** Stores the complete SPL data as a `JSONB` object in the database (converted from the source XML) for full auditing and data fidelity.
- **High-Performance Loading:** Utilizes native database bulk loading utilities (e.g., `COPY` in PostgreSQL) for maximum throughput.
- **Delta/Incremental Updates:** Intelligently downloads and processes only new or updated SPL archives, making it efficient to keep your database up-to-date.
- **Extensible by Design:** Built with an adapter pattern, allowing for the future addition of other database targets like Redshift, BigQuery, or Databricks.
- **Robust ETL Tracking:** All pipeline runs and processed files are tracked in the database for monitoring and idempotency.
- **Structured Logging:** Outputs logs in JSON format for easy integration with modern monitoring and log analysis platforms.

## Installation

The package requires Python 3.11+.

### 1. Install with PDM

This project uses `pdm` for dependency management.

```bash
pip install pdm
pdm install
```

### 2. Install with pip

You can also install the package and its dependencies using pip. The core package has minimal dependencies. Database-specific drivers must be installed as "extras".

```bash
# Install the core package
pip install .

# Install with the PostgreSQL driver
pip install .[postgresql]
```

## Configuration

The application is configured via environment variables. Create a `.env` file in the project root or set the variables in your shell.

**Required Settings:**

```dotenv
# .env file
# Database Configuration
DB_ADAPTER="postgresql"
DB_NAME="spl_data"
DB_USER="your_db_user"
DB_PASSWORD="your_db_password"
DB_HOST="localhost"
DB_PORT="5432"

# Path to store downloaded SPL zip archives
DOWNLOAD_PATH="/path/to/spl_downloads"

# Format for intermediate files (optional, defaults to "csv")
# Can be set to "csv" or "parquet". Parquet is recommended for performance
# and compatibility with cloud data warehouses.
INTERMEDIATE_FORMAT="csv"
```

## Usage (CLI)

The package provides a command-line interface (CLI) for running the ETL pipeline.

### 1. Initialize the Database Schema

Before running any data loads, you must initialize the database with the required tables and schemas.

```bash
pdm run py-load-spl init
```

### 2. Perform a Full Load

A full load populates the database with the entire FDA SPL dataset. This is typically done the first time you set up the database. The command can operate in two modes:

**Mode 1: Automatic Download (Recommended)**

If you run the command without any arguments, it will automatically download all SPL archives from the FDA source, unzip them, and process them. This is the easiest way to perform a full load.

```bash
pdm run py-load-spl full-load
```

**Mode 2: From a Local Directory**

If you have already downloaded and unzipped the SPL archives, you can point the tool to the directory containing the XML files using the `--source` option.

```bash
pdm run py-load-spl full-load --source /path/to/unzipped/xml/files
```

### 3. Perform a Delta (Incremental) Load

A delta load automatically identifies new SPL archives from the FDA source, downloads them, processes them, and loads them into the database. This is the standard command for keeping your database updated.

```bash
pdm run py-load-spl delta-load
```

The command will first check the `etl_processed_archives` table in your database to see what has already been processed, ensuring that no data is processed twice.

### Logging

By default, logs are output in a structured JSON format. You can switch to plain text for easier reading during development:

```bash
pdm run py-load-spl --log-format text delta-load
```
