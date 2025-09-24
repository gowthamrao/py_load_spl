import csv
import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import xmltodict
from pydantic import BaseModel

from . import schemas
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


# A mapping from our Pydantic models to their explicit PyArrow schemas.
MODEL_TO_SCHEMA_MAP: dict[type[BaseModel], pa.Schema] = {
    Product: schemas.PRODUCT_SCHEMA,
    Ingredient: schemas.INGREDIENT_SCHEMA,
    Packaging: schemas.PACKAGING_SCHEMA,
    MarketingStatus: schemas.MARKETING_STATUS_SCHEMA,
    ProductNdc: schemas.PRODUCT_NDC_SCHEMA,
    SplRawDocument: schemas.SPL_RAW_DOCUMENT_SCHEMA,
}


# A reverse mapping from a filename to its PyArrow schema for easy lookup.
FILENAME_TO_SCHEMA_MAP: dict[str, pa.Schema] = {
    filename: MODEL_TO_SCHEMA_MAP[model]
    for model, filename in MODEL_TO_FILENAME_MAP.items()
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
        self._csv_writers: dict[str, Any] = {}
        for _, name in MODEL_TO_FILENAME_MAP.items():
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
    """
    Writes data to Parquet files in batches using PyArrow to keep memory usage low.
    This implementation conforms to FRD requirement F005.4.
    """

    def __init__(self, output_dir: Path, batch_size: int = 100_000):
        super().__init__(output_dir)
        self.batch_size = batch_size
        self._batches: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._parquet_writers: dict[str, pq.ParquetWriter] = {}

    def _open(self) -> None:
        # ParquetWriter instances are created on-the-fly when the first batch for a
        # given file is written, so there's nothing to do here.
        pass

    def _close(self) -> None:
        """Writes any remaining records in the batches and closes all file writers."""
        logger.info("Closing Parquet writer and flushing final batches...")
        for name in list(self._batches.keys()):
            self._flush_batch(name)
        for writer in self._parquet_writers.values():
            writer.close()
        self._parquet_writers.clear()

    def _preprocess_batch(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Preprocesses a batch of records before writing to Parquet.
        This is where type conversions, like UUID to string, happen.
        """
        processed_batch = []
        for record in batch:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, UUID):
                    processed_record[key] = str(value)
                else:
                    processed_record[key] = value
            processed_batch.append(processed_record)
        return processed_batch

    def _flush_batch(self, name: str) -> None:
        """
        Writes the current in-memory batch to the corresponding Parquet file
        using its predefined, explicit schema.
        """
        batch = self._batches[name]
        if not batch:
            return

        logger.info(f"Flushing batch of {len(batch)} records to {name}.parquet...")
        processed_batch = self._preprocess_batch(batch)

        schema = FILENAME_TO_SCHEMA_MAP.get(name)
        if not schema:
            logger.error(f"No schema defined for Parquet file '{name}.parquet'")
            # Or raise an exception, but logging and skipping is more resilient
            return

        try:
            # Create the writer with the explicit schema if it doesn't exist
            if name not in self._parquet_writers:
                filepath = self.output_dir / f"{name}.parquet"
                self._parquet_writers[name] = pq.ParquetWriter(filepath, schema)

            # Create the Arrow Table using the explicit schema. This is critical
            # as it enforces the correct data types during table creation.
            table = pa.Table.from_pylist(processed_batch, schema=schema)

            self._parquet_writers[name].write_table(table)
            self._batches[name].clear()
        except Exception as e:
            logger.error(
                f"Failed to write Parquet batch for {name}. Error: {e}", exc_info=True
            )
            raise

    def write(self, model_instance: BaseModel) -> None:
        """
        Appends a model instance to the appropriate in-memory batch.
        If a batch reaches the defined `batch_size`, it is flushed to a file.
        """
        model_type = type(model_instance)
        file_base_name = MODEL_TO_FILENAME_MAP.get(model_type)
        if not file_base_name:
            raise TypeError(f"No Parquet mapping for model type: {model_type}")

        dumped = model_instance.model_dump()
        self._batches[file_base_name].append(dumped)
        self.stats[f"{file_base_name}.parquet"] += 1

        if len(self._batches[file_base_name]) >= self.batch_size:
            self._flush_batch(file_base_name)


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
