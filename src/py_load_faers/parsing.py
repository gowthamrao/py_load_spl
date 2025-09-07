import csv
import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Schemas inferred from the header rows of the 2025Q2 ASCII files
FAERS_ASCII_SCHEMAS = {
    "DEMO": [
        "primaryid", "caseid", "caseversion", "i_f_code", "event_dt", "mfr_dt",
        "init_fda_dt", "fda_dt", "rept_cod", "auth_num", "mfr_num", "mfr_sndr",
        "lit_ref", "age", "age_cod", "age_grp", "sex", "e_sub", "wt", "wt_cod",
        "rept_dt", "to_mfr", "occp_cod", "reporter_country", "occr_country",
    ],
    "DRUG": [
        "primaryid", "caseid", "drug_seq", "role_cod", "drugname", "prod_ai",
        "val_vbm", "route", "dose_vbm", "cum_dose_chr", "cum_dose_unit",
        "dechal", "rechal", "lot_num", "exp_dt", "nda_num", "dose_amt",
        "dose_unit", "dose_form", "dose_freq",
    ],
    "REAC": ["primaryid", "caseid", "pt", "drug_rec_act"],
    "OUTC": ["primaryid", "caseid", "outc_cod"],
    "RPSR": ["primaryid", "caseid", "rpsr_cod"],
    "THER": [
        "primaryid", "caseid", "dsg_drug_seq", "start_dt", "end_dt", "dur",
        "dur_cod",
    ],
    "INDI": ["primaryid", "caseid", "indi_drug_seq", "indi_pt"],
}


def _find_data_files(source_dir: Path) -> dict[str, Path]:
    """Finds the 7 FAERS ASCII data files in a directory."""
    found_files = {}
    for table_name in FAERS_ASCII_SCHEMAS:
        # Files are named like 'DEMO25Q2.txt'
        files = list(source_dir.glob(f"{table_name}*.txt"))
        if not files:
            raise FileNotFoundError(f"No data file found for table {table_name} in {source_dir}")
        if len(files) > 1:
            logger.warning(f"Multiple files found for {table_name}, using first: {files[0]}")
        found_files[table_name] = files[0]
    return found_files


def stream_ascii_records(
    source_dir: Path,
) -> Generator[dict[str, Any], None, None]:
    """
    Streams records from the 7 FAERS ASCII files in a given quarter's directory.

    This function implements the core parsing logic for FRD R6.

    Args:
        source_dir: The path to the directory containing the ASCII .txt files.

    Yields:
        A dictionary for each row in each file, with table name and data.
        Example: {'table': 'DEMO', 'data': {'primaryid': '123', ...}}
    """
    data_files = _find_data_files(source_dir)
    for table_name, filepath in data_files.items():
        logger.info(f"Streaming records from {filepath} for table {table_name}...")
        schema = FAERS_ASCII_SCHEMAS[table_name]

        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            # The files are not strictly well-formed CSVs (e.g., unquoted newlines)
            # but csv.reader with '$' delimiter is robust enough for most cases.
            reader = csv.reader(f, delimiter="$")

            # Skip header row
            next(reader, None)

            for i, row in enumerate(reader):
                # Some rows may have trailing delimiters, creating extra empty fields.
                # We'll truncate the row to match the schema length.
                if len(row) > len(schema):
                    row = row[:len(schema)]

                # Create a dictionary from the schema and the row data
                record = dict(zip(schema, row))

                yield {"table": table_name, "data": record}
        logger.info(f"Finished streaming from {filepath}.")


def parse_and_stage_ascii_quarter(source_dir: Path, staging_dir: Path) -> dict[str, int]:
    """
    Parses a full FAERS ASCII quarter and writes the output to 7 intermediate
    CSV files in a staging directory. Implements FRD R9.

    Args:
        source_dir: The directory containing the source .txt files.
        staging_dir: The directory where the output CSVs will be written.

    Returns:
        A dictionary with the counts of rows processed for each table.
    """
    logger.info(f"Starting ASCII parsing and staging from {source_dir} to {staging_dir}")
    staging_dir.mkdir(parents=True, exist_ok=True)

    file_handles = {}
    writers = {}
    counts = {table: 0 for table in FAERS_ASCII_SCHEMAS}

    try:
        # Open a file for each table
        for table_name, schema in FAERS_ASCII_SCHEMAS.items():
            filepath = staging_dir / f"{table_name}.csv"
            f = open(filepath, "w", newline="", encoding="utf-8")
            file_handles[table_name] = f

            writer = csv.DictWriter(f, fieldnames=schema)
            writer.writeheader()
            writers[table_name] = writer

        # Stream records and write to the appropriate file
        for record in stream_ascii_records(source_dir):
            table = record["table"]
            data = record["data"]
            writers[table].writerow(data)
            counts[table] += 1

    finally:
        # Ensure all file handles are closed
        for f in file_handles.values():
            f.close()

    logger.info("Finished staging.")
    for table, count in counts.items():
        logger.info(f"  - {table}: {count} rows")

    return counts
