from pathlib import Path

import pytest
from memory_profiler import memory_usage

from py_load_spl.parsing import parse_spl_file


def create_large_spl_file(tmp_path: Path, size_in_mb: int) -> Path:
    """Creates a large, fake SPL XML file for memory testing."""
    file_path = tmp_path / "large_spl.xml"
    content_to_repeat = """
    <component>
        <section>
            <part>
                <code code="NDC-1" />
                <name>Some text to make the file larger and larger and larger.</name>
            </part>
        </section>
    </component>
    """
    repeat_count = (size_in_mb * 1024 * 1024) // len(content_to_repeat)

    with file_path.open("w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<document xmlns="urn:hl7-org:v3">\n')
        f.write('<id root="large-file-test"/>\n')
        f.write(
            "<subject><manufacturedProduct><manufacturedProduct/></manufacturedProduct></subject>"
        )
        f.write("<component><structuredBody>")
        for _ in range(repeat_count):
            f.write(content_to_repeat)
        f.write("</structuredBody></component>")
        f.write("</document>\n")
    return file_path


@pytest.fixture
def large_spl_file(tmp_path: Path) -> Path:
    return create_large_spl_file(tmp_path, 5)


@pytest.mark.xfail(
    reason="Memory inefficiency in parser. The current parser loads the entire file "
    "into memory via read_text() and then builds a full lxml tree, causing "
    "high memory usage for large files. A streaming parsing approach is needed "
    "to fix this, which is a major refactoring."
)
def test_parse_spl_file_memory_efficiency(large_spl_file: Path) -> None:
    """
    Tests that parse_spl_file uses a constant, low amount of memory.
    This test is expected to fail until the parser is refactored.
    """
    file_size_mb = large_spl_file.stat().st_size / (1024 * 1024)
    mem_usage = memory_usage(
        (parse_spl_file, (large_spl_file,)), interval=0.1, timeout=200
    )
    memory_increase_mb = max(mem_usage) - mem_usage[0]

    print(f"File size: {file_size_mb:.2f} MB")
    print(f"Memory increase: {memory_increase_mb:.2f} MiB")

    # The memory increase should be much smaller than the file size.
    # This assertion will FAIL with the current implementation.
    assert memory_increase_mb < 1
