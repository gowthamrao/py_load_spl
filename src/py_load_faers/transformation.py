import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def transform_data(
    parsed_data_stream: Iterable[dict[str, Any]], output_dir: Path
) -> None:
    """
    Placeholder for the transformation layer.

    This function will implement F003, F004, and F005.
    """
    logger.info("Starting data transformation...")
    # TODO: Implement F003 - Map to normalized relational schema.
    # TODO: Implement F004 - Capture full original XML.
    # TODO: Implement F005 - Generate intermediate files (e.g., CSV).

    # Example of processing the stream
    record_count = 0
    for record in parsed_data_stream:
        logger.debug(f"Transforming record with doc_id: {record.get('document_id')}")
        # Here we would:
        # 1. Validate the record (e.g., with a Pydantic model).
        # 2. Map to different tables (PRODUCTS, INGREDIENTS, etc.).
        # 3. Write to intermediate CSV files in batches.
        # 4. Write raw data to a separate file.
        record_count += 1

    logger.info(
        f"Transformation placeholder completed. Processed {record_count} records."
    )
    # Create dummy output files to simulate the process
    (output_dir / "products.csv").touch()
    (output_dir / "ingredients.csv").touch()
    (output_dir / "spl_raw_documents.csv").touch()
