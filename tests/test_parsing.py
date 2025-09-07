from pathlib import Path
import csv

import pytest

from src.py_load_faers import parsing

# Define synthetic data for our tests.
# Using a subset of fields for simplicity, as we just need to test the parser's mechanics.
SYNTHETIC_DATA = {
    "DEMO": ["primaryid$caseid$sex", "101$1$M", "102$2$F"],
    "DRUG": ["primaryid$drug_seq$drugname", "101$1$Aspirin", "101$2$Tylenol", "102$1$Motrin"],
    "REAC": ["primaryid$pt", "101$Headache", "102$Fever"],
    "OUTC": ["primaryid$outc_cod", "101$OT", "102$DE"],
    "RPSR": ["primaryid$rpsr_cod", "101$MD"],
    "THER": ["primaryid$start_dt", "101$20250101"],
    "INDI": ["primaryid$indi_pt", "101$Pain"],
}

# Manually define schemas for the synthetic data to match the test data.
# In a real scenario, this would be more comprehensive.
TEST_SCHEMAS = {
    "DEMO": ["primaryid", "caseid", "sex"],
    "DRUG": ["primaryid", "drug_seq", "drugname"],
    "REAC": ["primaryid", "pt"],
    "OUTC": ["primaryid", "outc_cod"],
    "RPSR": ["primaryid", "rpsr_cod"],
    "THER": ["primaryid", "start_dt"],
    "INDI": ["primaryid", "indi_pt"],
}


@pytest.fixture
def faers_test_data_dir(tmp_path: Path) -> Path:
    """Creates a temporary directory with a full set of synthetic FAERS data."""
    data_dir = tmp_path / "faers_ascii"
    data_dir.mkdir()
    for table_name, lines in SYNTHETIC_DATA.items():
        content = "\n".join(lines)
        # Create a file like DEMO25Q2.txt to match the real naming scheme
        (data_dir / f"{table_name}25Q2.txt").write_text(content)
    return data_dir


def test_stream_ascii_records(faers_test_data_dir: Path, mocker):
    """
    Tests the streaming parser to ensure it reads and yields data correctly.
    """
    # Mock the real schemas to match our simplified test schemas
    mocker.patch.dict(parsing.FAERS_ASCII_SCHEMAS, TEST_SCHEMAS)

    records = list(parsing.stream_ascii_records(faers_test_data_dir))

    # Total records should be the sum of data rows in SYNTHETIC_DATA
    # 2+3+2+2+1+1+1 = 12
    assert len(records) == 12

    # Spot check a few records
    demo_record = next(r for r in records if r["table"] == "DEMO" and r["data"]["primaryid"] == "102")
    assert demo_record["data"]["caseid"] == "2"
    assert demo_record["data"]["sex"] == "F"

    drug_record = next(r for r in records if r["table"] == "DRUG" and r["data"]["drugname"] == "Tylenol")
    assert drug_record["data"]["primaryid"] == "101"
    assert drug_record["data"]["drug_seq"] == "2"


def test_parse_and_stage_ascii_quarter(faers_test_data_dir: Path, mocker):
    """
    Tests the end-to-end parsing and staging process.
    """
    mocker.patch.dict(parsing.FAERS_ASCII_SCHEMAS, TEST_SCHEMAS)
    staging_dir = faers_test_data_dir.parent / "staged"

    counts = parsing.parse_and_stage_ascii_quarter(faers_test_data_dir, staging_dir)

    assert counts["DEMO"] == 2
    assert counts["DRUG"] == 3
    assert counts["REAC"] == 2
    assert counts["OUTC"] == 2
    assert counts["RPSR"] == 1
    assert counts["THER"] == 1
    assert counts["INDI"] == 1

    # Verify the content of one of the created CSV files
    demo_csv_path = staging_dir / "DEMO.csv"
    assert demo_csv_path.exists()

    with open(demo_csv_path, "r") as f:
        reader = csv.reader(f)
        lines = list(reader)

    # Header + 2 data rows
    assert len(lines) == 3
    assert lines[0] == ["primaryid", "caseid", "sex"]
    # Data rows can be in any order
    assert ["101", "1", "M"] in lines
    assert ["102", "2", "F"] in lines
