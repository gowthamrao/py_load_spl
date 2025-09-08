from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Literal


class DatabaseLoader(ABC):
    """
    Abstract Base Class for all database loaders, as defined in the FRD (Sec 2.2).

    This class defines the contract that all database-specific adapters must follow.
    """

    @abstractmethod
    def initialize_schema(self) -> None:
        """Creates the necessary tables and structures (F008.3)."""
        pass

    @abstractmethod
    def bulk_load_to_staging(self, intermediate_dir: Path) -> None:
        """
        Loads the intermediate files into staging tables using native utilities
        (F006.1, F006.2).
        """
        pass

    @abstractmethod
    def pre_load_optimization(self) -> None:
        """Optional: Drop indexes, disable constraints (for full loads)."""
        pass

    @abstractmethod
    def merge_from_staging(self, mode: Literal["full-load", "delta-load"]) -> None:
        """
        Atomically merges (UPSERT) or swaps (FULL LOAD) data from staging to
        production tables (F006.3).
        """
        pass

    @abstractmethod
    def post_load_cleanup(self) -> None:
        """Optional: Rebuild indexes, enable constraints, vacuum/analyze."""
        pass

    @abstractmethod
    def start_run(self, mode: Literal["full-load", "delta-load"]) -> int:
        """
        Creates a new entry in the ETL history table for the current run
        and returns a unique run identifier.
        """
        pass

    @abstractmethod
    def end_run(
        self, run_id: int, status: str, records_loaded: int, error_log: str | None
    ) -> None:
        """
        Updates the ETL history table for the specified run with its final
        status and metrics.
        """
        pass
