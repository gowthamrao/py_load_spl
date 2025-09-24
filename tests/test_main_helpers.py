from pathlib import Path
import pytest

from py_load_spl.config import (
    DatabricksSettings,
    PostgresSettings,
    RedshiftSettings,
    S3Settings,
    Settings,
    SqliteSettings,
)
from py_load_spl.db.databricks import DatabricksLoader
from py_load_spl.db.postgres import PostgresLoader
from py_load_spl.db.redshift import RedshiftLoader
from py_load_spl.db.sqlite import SqliteLoader
from py_load_spl.main import get_db_loader, get_file_writer
from py_load_spl.transformation import CsvWriter, ParquetWriter


def test_get_db_loader_postgresql() -> None:
    """Tests that the correct loader is returned for postgresql."""
    settings = Settings(db=PostgresSettings(adapter="postgresql"))
    loader = get_db_loader(settings)
    assert isinstance(loader, PostgresLoader)


def test_get_db_loader_sqlite() -> None:
    """Tests that the correct loader is returned for sqlite."""
    settings = Settings(db=SqliteSettings(adapter="sqlite", name="test.db"))
    loader = get_db_loader(settings)
    assert isinstance(loader, SqliteLoader)


def test_get_db_loader_redshift() -> None:
    """Tests that the correct loader is returned for redshift."""
    settings = Settings(
        db=RedshiftSettings(
            adapter="redshift",
            host="test",
            user="test",
            password="test",
            iam_role_arn="test",
        ),
        s3=S3Settings(bucket="test", prefix="test"),
    )
    loader = get_db_loader(settings)
    assert isinstance(loader, RedshiftLoader)


def test_get_db_loader_databricks() -> None:
    """Tests that the correct loader is returned for databricks."""
    settings = Settings(
        db=DatabricksSettings(
            adapter="databricks",
            server_hostname="test",
            http_path="test",
            token="test",
            s3_staging_path="s3://test/test",
        )
    )
    loader = get_db_loader(settings)
    assert isinstance(loader, DatabricksLoader)


def test_get_db_loader_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests that a ValueError is raised for an unsupported adapter."""
    settings = Settings(db=PostgresSettings(adapter="postgresql"))
    # Bypass pydantic validation to test the function's logic
    monkeypatch.setattr(settings.db, "adapter", "unsupported")
    with pytest.raises(ValueError, match="Unsupported DB adapter 'unsupported'"):
        get_db_loader(settings)


def test_get_file_writer_csv(tmp_path: Path) -> None:
    """Tests that the CsvWriter is returned for the 'csv' format."""
    settings = Settings(db=PostgresSettings(adapter="postgresql"))
    # Ensure the default is tested
    settings.intermediate_format = "csv"
    writer = get_file_writer(settings, tmp_path)
    assert isinstance(writer, CsvWriter)


def test_get_file_writer_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that the ParquetWriter is returned for the 'parquet' format."""
    settings = Settings(db=PostgresSettings(adapter="postgresql"))
    monkeypatch.setattr(settings, "intermediate_format", "parquet")
    writer = get_file_writer(settings, tmp_path)
    assert isinstance(writer, ParquetWriter)


def test_get_file_writer_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tests that a ValueError is raised for an unsupported format."""
    settings = Settings(db=PostgresSettings(adapter="postgresql"))
    monkeypatch.setattr(settings, "intermediate_format", "unsupported")
    with pytest.raises(
        ValueError, match="Unsupported intermediate format 'unsupported'"
    ):
        get_file_writer(settings, tmp_path)
