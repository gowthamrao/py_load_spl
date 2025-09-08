from pathlib import Path

import pytest

from py_load_spl.parsing import parse_spl_file

# Get the directory of the current test file
TEST_DIR = Path(__file__).parent


def test_parsing_multiple_marketing_statuses():
    """
    Tests that the parser correctly extracts multiple marketing status entries,
    including those with and without an end date.

    This test is designed to FAIL with the old parsing logic.
    """
    # Path to the new, more complex sample file
    test_file = TEST_DIR / "data" / "sample_spl_with_multiple_marketing_statuses.xml"

    # Parse the file
    parsed_data = parse_spl_file(test_file)

    # Check that the 'marketing_status' key exists and is a list
    assert "marketing_status" in parsed_data
    assert isinstance(parsed_data["marketing_status"], list)

    # The old parser would find 1, the new one should find 2
    assert len(parsed_data["marketing_status"]) == 2

    # Sort the results by start_date to have a predictable order for assertions
    # The old parser would fail here as 'end_date' key is missing.
    # We add a default `None` to allow sorting even if the key is missing.
    statuses = sorted(parsed_data["marketing_status"], key=lambda x: x.get("start_date"))

    # Assertions for the first marketing status (completed)
    assert statuses[0]["marketing_category"] == "completed"
    assert statuses[0]["start_date"] == "20240101"
    assert statuses[0]["end_date"] == "20241231"  # This is the key missing field

    # Assertions for the second marketing status (active)
    assert statuses[1]["marketing_category"] == "active"
    assert statuses[1]["start_date"] == "20250101"
    assert statuses[1]["end_date"] is None  # Should be None when <high> is not present
