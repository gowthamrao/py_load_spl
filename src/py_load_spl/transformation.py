import csv
import json
import logging
import xmltodict
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
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

# A mapping from our Pydantic models to the output filenames (without extension).
MODEL_TO_FILENAME_MAP: dict[type[BaseModel], str] = {
    Product: "products",
    Ingredient: "ingredients",
    Packaging: "packaging",
    MarketingStatus: "marketing_status",
    ProductNdc: "product_ndcs",
    SplRawDocument: "spl_raw_documents",
}

# --- Writer Abstraction ---


class FileWriter(ABC):
    """Abstract base class for file writers."""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.stats: dict[str, int] = defaultdict(int)

    def __enter__(self) -> "FileWriter":
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self._close()
        logger.info(f"Writer closed. Total rows written: {sum(self.stats.values())}")

    @abstractmethod
    def _open(self) -> None:
        """Initializes resources (e.g., opens file handles)."""
        pass

    @abstractmethod
    def _close(self) -> None:
        """Cleans up resources (e.g., closes file handles)."""
        pass

    @abstractmethod
    def write(self, model_instance: BaseModel) -> None:
        """Writes a single Pydantic model instance."""
        pass


class CsvWriter(FileWriter):
    """Writes data to CSV files, optimized for PostgreSQL COPY."""

    def _open(self) -> None:
        self._file_handles: dict[str, IO[str]] = {}
        self._csv_writers: dict[str, csv.writer] = {}
        for model_cls, name in MODEL_TO_FILENAME_MAP.items():
            filename = f"{name}.csv"
            filepath = self.output_dir / filename
            file_handle = open(filepath, "w", newline="", encoding="utf-8")
            self._file_handles[filename] = file_handle
            self._csv_writers[filename] = csv.writer(
                file_handle, quoting=csv.QUOTE_MINIMAL
            )

    def _close(self) -> None:
        for handle in self._file_handles.values():
            handle.close()

    def write(self, model_instance: BaseModel) -> None:
        model_type = type(model_instance)
        file_base_name = MODEL_TO_FILENAME_MAP.get(model_type)
        if not file_base_name:
            raise TypeError(f"No CSV mapping for model type: {model_type}")

        filename = f"{file_base_name}.csv"
        writer = self._csv_writers[filename]

        dumped = model_instance.model_dump()

        row = ["\\N" if v is None else v for v in dumped.values()]
        writer.writerow(row)
        self.stats[filename] += 1


class ParquetWriter(FileWriter):
    """Writes data to Parquet files using PyArrow."""

    def _open(self) -> None:
        self._batches: dict[str, list[dict]] = defaultdict(list)

    def _close(self) -> None:
        """Converts batches to strings and writes them to Parquet files."""
        for name, batch in self._batches.items():
            if not batch:
                continue

            # Bug Fix: Convert UUIDs to strings before writing to Parquet
            processed_batch = []
            for record in batch:
                processed_record = {}
                for key, value in record.items():
                    if isinstance(value, UUID):
                        processed_record[key] = str(value)
                    else:
                        processed_record[key] = value
                processed_batch.append(processed_record)

            try:
                table = pa.Table.from_pylist(processed_batch)
                filepath = self.output_dir / f"{name}.parquet"
                pq.write_table(table, filepath)
            except Exception as e:
                logger.error(f"Failed to write Parquet file for {name}. Error: {e}")

    def write(self, model_instance: BaseModel) -> None:
        model_type = type(model_instance)
        file_base_name = MODEL_TO_FILENAME_MAP.get(model_type)
        if not file_base_name:
            raise TypeError(f"No Parquet mapping for model type: {model_type}")

        dumped = model_instance.model_dump()

        self._batches[file_base_name].append(dumped)
        self.stats[f"{file_base_name}.parquet"] += 1


# --- Transformer ---


class Transformer:
    """
    Implements the transformation logic (F003, F005).

    Takes parsed data, validates it with Pydantic models, and uses a
    FileWriter to persist the data to an intermediate format.
    """

    def __init__(self, writer: FileWriter):
        self.writer = writer
        logger.info(f"Transformer initialized with writer: {type(writer).__name__}")

    def transform_stream(
        self, parsed_data_stream: Iterable[dict[str, Any]]
    ) -> dict[str, int]:
        """
        Processes a stream of parsed data, writes it via the writer, and returns statistics.
        """
        logger.info("Starting data transformation stream processing...")
        with self.writer as writer:
            for i, record in enumerate(parsed_data_stream):
                doc_id = record.get("document_id")
                if not doc_id:
                    logger.warning(
                        f"Skipping record due to missing document_id: {record}"
                    )
                    continue

                try:
                    # Centralize the XML to JSON conversion here
                    raw_doc_model = SplRawDocument.model_validate(record)
                    if raw_doc_model.raw_data:
                        try:
                            # Parse XML to dict, then dump to JSON string
                            xml_dict = xmltodict.parse(raw_doc_model.raw_data)
                            raw_doc_model.raw_data = json.dumps(xml_dict)
                        except Exception as e:
                            logger.error(
                                f"Failed to parse XML and convert to JSON for "
                                f"doc_id {doc_id}. Error: {e}"
                            )
                            # Store as null or an error marker instead of failing
                            raw_doc_model.raw_data = None

                    # Validate and write all model types from the single source record
                    writer.write(Product.model_validate(record))
                    writer.write(raw_doc_model)  # Write the modified raw_doc_model

                    for ing_data in record.get("ingredients", []):
                        writer.write(Ingredient(document_id=doc_id, **ing_data))
                    for pkg_data in record.get("packaging", []):
                        writer.write(Packaging(document_id=doc_id, **pkg_data))
                    for mkt_data in record.get("marketing_status", []):
                        writer.write(MarketingStatus(document_id=doc_id, **mkt_data))
                    for ndc_data in record.get("product_ndcs", []):
                        writer.write(ProductNdc(document_id=doc_id, **ndc_data))

                except Exception as e:
                    logger.error(
                        f"Failed to transform record with doc_id {doc_id}. Error: {e}"
                    )
                    continue

                if (i + 1) % 1000 == 0:
                    logger.info(f"Processed {i + 1} source documents...")

        logger.info(
            f"Transformation complete. Total XMLs processed: {self.writer.stats.get('products.csv', self.writer.stats.get('products.parquet', 0))}"
        )
        return dict(self.writer.stats)
