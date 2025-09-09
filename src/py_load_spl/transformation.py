import csv
import logging
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any, IO

from pydantic import BaseModel

from .models import (
    Ingredient,
    MarketingStatus,
    Packaging,
    Product,
    ProductNdc,
    SplRawDocument,
)

logger = logging.getLogger(__name__)

# A mapping from our Pydantic models to the output CSV filenames.
MODEL_TO_FILENAME_MAP = {
    Product: "products.csv",
    Ingredient: "ingredients.csv",
    Packaging: "packaging.csv",
    MarketingStatus: "marketing_status.csv",
    ProductNdc: "product_ndcs.csv",
    SplRawDocument: "spl_raw_documents.csv",
}


class CsvWriterManager:
    """
    Manages the file handles and CSV writers for all output files.

    This class ensures that rows are written to the correct files and that
    all files are properly closed upon exit.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self._file_handles: dict[str, IO[str]] = {}
        self._csv_writers: dict[str, csv.writer] = {}

    def __enter__(self) -> "CsvWriterManager":
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for model_cls, filename in MODEL_TO_FILENAME_MAP.items():
            filepath = self.output_dir / filename
            # Keep a reference to the file handle to close it later
            file_handle = open(filepath, "w", newline="", encoding="utf-8")
            self._file_handles[filename] = file_handle
            # Create a CSV writer for that file
            self._csv_writers[filename] = csv.writer(
                file_handle, quoting=csv.QUOTE_MINIMAL
            )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        for handle in self._file_handles.values():
            handle.close()
        logger.info("All CSV output files closed.")

    def write_row(self, model_instance: BaseModel) -> str:
        """
        Writes a Pydantic model instance to the corresponding CSV file.
        Returns the name of the file written to for stats tracking.
        """
        import json
        from .models import SplRawDocument

        filename = MODEL_TO_FILENAME_MAP.get(type(model_instance))
        if not filename:
            raise TypeError(f"No CSV mapping for model type: {type(model_instance)}")

        writer = self._csv_writers[filename]

        dumped = model_instance.model_dump()

        # Special handling for the raw_data field to ensure it's a valid JSON string literal
        if isinstance(model_instance, SplRawDocument) and "raw_data" in dumped:
            dumped["raw_data"] = json.dumps(dumped["raw_data"])

        # Convert Pydantic model to a list of values, replacing None with \N for Postgres COPY
        row = ["\\N" if v is None else v for v in dumped.values()]
        writer.writerow(row)
        return filename


class Transformer:
    """
    Implements the transformation logic (F003, F005).

    Takes parsed data, validates it with Pydantic models, and writes the
    normalized data to intermediate CSV files suitable for bulk loading.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        logger.info(f"Transformer initialized. Output will be written to {output_dir}")

    def transform_stream(
        self, parsed_data_stream: Iterable[dict[str, Any]]
    ) -> dict[str, int]:
        """
        Processes a stream of parsed data, writes it to CSV files, and returns statistics.

        Args:
            parsed_data_stream: An iterable of dictionaries, where each dictionary
                                represents one parsed SPL document.

        Returns:
            A dictionary with counts of each record type processed.
        """
        logger.info("Starting data transformation stream processing...")
        stats: dict[str, int] = defaultdict(int)
        with CsvWriterManager(self.output_dir) as writer_manager:
            for i, record in enumerate(parsed_data_stream):
                doc_id = record.get("document_id")
                if not doc_id:
                    logger.warning(f"Skipping record due to missing document_id: {record}")
                    continue

                try:
                    # 1. Transform and write the main Product record
                    product = Product.model_validate(record)
                    stats[writer_manager.write_row(product)] += 1

                    # 2. Transform and write the SplRawDocument record
                    raw_doc = SplRawDocument.model_validate(record)
                    stats[writer_manager.write_row(raw_doc)] += 1

                    # 3. Transform and write one-to-many Ingredient records
                    for ing_data in record.get("ingredients", []):
                        ingredient = Ingredient(document_id=doc_id, **ing_data)
                        stats[writer_manager.write_row(ingredient)] += 1

                    # 4. Transform and write one-to-many Packaging records
                    for pkg_data in record.get("packaging", []):
                        packaging = Packaging(document_id=doc_id, **pkg_data)
                        stats[writer_manager.write_row(packaging)] += 1

                    # 5. Transform and write one-to-many MarketingStatus records
                    for mkt_data in record.get("marketing_status", []):
                        status = MarketingStatus(document_id=doc_id, **mkt_data)
                        stats[writer_manager.write_row(status)] += 1

                    # 6. Transform and write one-to-many ProductNdc records
                    for ndc_data in record.get("product_ndcs", []):
                        ndc = ProductNdc(document_id=doc_id, **ndc_data)
                        stats[writer_manager.write_row(ndc)] += 1

                except Exception as e:
                    logger.error(f"Failed to transform record with doc_id {doc_id}. Error: {e}")
                    # Continue processing other records
                    continue

                if (i + 1) % 1000 == 0:
                    logger.info(f"Processed {i + 1} source documents...")

        total_records = sum(stats.values())
        logger.info(
            f"Transformation complete. Total XMLs processed: {stats.get('products.csv', 0)}. Total rows created: {total_records}"
        )
        return dict(stats)
