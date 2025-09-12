from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal


class DatabaseLoader(ABC):
    """
    Abstract Base Class for all database loaders, as defined in the FRD (Sec 2.2).

    This class defines the contract that all database-specific adapters must follow.
    """

    @abstractmethod
    def initialize_schema(self) -> None:  # pragma: no cover
        """Creates the necessary tables and structures (F008.3)."""
        pass

    @abstractmethod
    def bulk_load_to_staging(self, intermediate_dir: Path) -> int:  # pragma: no cover
        """
        Loads intermediate files into staging tables using native utilities.

        :return: The total number of rows loaded into all staging tables.
        """
        pass

    @abstractmethod
    def pre_load_optimization(
        self, mode: Literal["full-load", "delta-load"]
    ) -> None:  # pragma: no cover
        """Optional: Drop indexes, disable constraints (for full loads)."""
        pass

    @abstractmethod
    def merge_from_staging(
        self, mode: Literal["full-load", "delta-load"]
    ) -> None:  # pragma: no cover
        """
        Atomically merges (UPSERT) or swaps (FULL LOAD) data from staging to
        production tables (F006.3).
        """
        pass

    @abstractmethod
    def post_load_cleanup(
        self, mode: Literal["full-load", "delta-load"]
    ) -> None:  # pragma: no cover
        """Optional: Rebuild indexes, enable constraints, vacuum/analyze."""
        pass

    @abstractmethod
    def start_run(
        self, mode: Literal["full-load", "delta-load"]
    ) -> int:  # pragma: no cover
        """
        Creates a new entry in the ETL history table for the current run
        and returns a unique run identifier.
        """
        pass

    @abstractmethod
    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:  # pragma: no cover
        """
        Updates the ETL history table for the specified run with its final
        status and metrics.
        """
        pass

    @abstractmethod
    def get_processed_archives(self) -> set[str]:  # pragma: no cover
        """
        Retrieves the set of archive names that have already been
        successfully processed.
        """
        pass

    @abstractmethod
    def record_processed_archive(
        self, archive_name: str, checksum: str
    ) -> None:  # pragma: no cover
        """
        Records a single archive as successfully processed in the database,
        typically after a successful ETL cycle for that archive.
        """
        pass
