import concurrent.futures
import logging
import shutil
import tempfile
from concurrent.futures import as_completed
from pathlib import Path

import typer
from rich.console import Console

from . import __name__ as app_name
from .config import Settings, get_settings
from .db.base import DatabaseLoader
from .db.postgres import PostgresLoader
from .parsing import parse_spl_file
from .transformation import (
    CsvWriter,
    FileWriter,
    ParquetWriter,
    Transformer,
)
from .util import setup_logging

app = typer.Typer(name=app_name)
console = Console()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    log_level: str = typer.Option(
        "INFO", help="Set the logging level.", envvar="LOG_LEVEL"
    ),
    log_format: str = typer.Option(
        "json", help="Set the log format ('json' or 'text').", envvar="LOG_FORMAT"
    ),
    intermediate_format: str = typer.Option(
        "csv",
        help="Format for intermediate files ('csv' or 'parquet').",
        envvar="INTERMEDIATE_FORMAT",
    ),
) -> None:
    """A CLI for the SPL Data Loader."""
    # Note: intermediate_format is also handled by Pydantic, but adding it here
    # makes it easily discoverable via --help.
    setup_logging(log_level, log_format)
    settings = get_settings()
    # Allow CLI option to override environment variable for format
    if intermediate_format in ["csv", "parquet"]:
        settings.intermediate_format = intermediate_format
    ctx.obj = settings
    if ctx.invoked_subcommand is None:
        console.print("[bold red]No command specified. Use --help for options.[/bold red]")


def get_db_loader(settings: Settings) -> DatabaseLoader:
    if settings.db.adapter == "postgresql":
        return PostgresLoader(settings.db)
    else:
        console.print(f"[bold red]Error: Unsupported DB adapter '{settings.db.adapter}'[/bold red]")
        raise typer.Exit(1)


def get_file_writer(settings: Settings, output_dir: Path) -> FileWriter:
    """Instantiates the correct file writer based on settings."""
    if settings.intermediate_format == "parquet":
        console.print("[bold blue]Using Parquet format for intermediate files.[/bold blue]")
        return ParquetWriter(output_dir)
    elif settings.intermediate_format == "csv":
        console.print("[bold blue]Using CSV format for intermediate files.[/bold blue]")
        return CsvWriter(output_dir)
    else:
        # This case should be prevented by Pydantic validation, but as a safeguard:
        console.print(
            f"[bold red]Error: Unsupported intermediate format '{settings.intermediate_format}'[/bold red]"
        )
        raise typer.Exit(1)


def _quarantine_and_parse_in_parallel(
    xml_files: list[Path], settings: Settings, executor: concurrent.futures.ProcessPoolExecutor
):
    """
    Parses a list of XML files in parallel, quarantining any file that fails.
    Yields successfully parsed data dictionaries.
    """
    futures = {executor.submit(parse_spl_file, file): file for file in xml_files}
    quarantined_count = 0

    for future in as_completed(futures):
        source_file_path = futures[future]
        try:
            yield future.result()
        except Exception as e:
            quarantine_dir = Path(settings.quarantine_path)
            quarantine_dir.mkdir(parents=True, exist_ok=True)
            target_path = quarantine_dir / source_file_path.name
            if source_file_path.exists():
                shutil.move(str(source_file_path), str(target_path))
                quarantined_count += 1
                logging.warning(
                    f"Moved corrupted file {source_file_path.name} to {target_path} due to parsing error: {e}"
                )
            else:
                logging.warning(
                    f"Could not quarantine {source_file_path.name} as it was already moved or deleted."
                )

    if quarantined_count > 0:
        console.print(
            f"[bold yellow]Quarantined {quarantined_count} file(s).[/bold yellow]"
        )


@app.command()
def init(ctx: typer.Context) -> None:
    """F008.3: Initialize the database schema."""
    console.print("[bold green]Initializing database schema...[/bold green]")
    settings: Settings = ctx.obj
    loader = get_db_loader(settings)
    try:
        loader.initialize_schema()
        console.print("[bold green]Schema initialization complete.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Schema initialization failed: {e}[/bold red]")
        raise typer.Exit(1)


@app.command()
def full_load(
    ctx: typer.Context,
    source: Path = typer.Option(
        ...,
        help="Local path to the directory containing SPL XML files.",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
    ),
) -> None:
    """F008.3: Perform a full data load from a local directory."""
    settings: Settings = ctx.obj
    console.print(f"[bold cyan]Starting full data load from '{source}'...[/bold cyan]")
    loader = get_db_loader(settings)
    run_id = None
    try:
        run_id = loader.start_run(mode="full-load")
        with tempfile.TemporaryDirectory() as temp_dir_str:
            output_dir = Path(temp_dir_str)
            console.print(f"Intermediate files will be stored in: {output_dir}")

            writer = get_file_writer(settings, output_dir)

            console.print("[cyan]Step 1: Finding XML files...[/cyan]")
            xml_files = list(source.glob("**/*.xml"))
            console.print(f"Found {len(xml_files)} XML files to process.")

            console.print(
                f"[cyan]Step 2: Parsing and Transforming in parallel (max_workers={settings.max_workers})...[/cyan]"
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = _quarantine_and_parse_in_parallel(
                    xml_files, settings, executor
                )
                transformer = Transformer(writer=writer)
                stats = transformer.transform_stream(parsed_data_stream)

            console.print("[green]Parsing and Transformation complete.[/green]")

            console.print("[cyan]Step 3: Loading data into database...[/cyan]")
            loader.pre_load_optimization(mode="full-load")
            loader.bulk_load_to_staging(output_dir)
            loader.merge_from_staging("full-load")
            loader.post_load_cleanup(mode="full-load")
            console.print("[green]Database loading complete.[/green]")

        if run_id:
            total_records = sum(stats.values()) if stats else 0
            loader.end_run(run_id, "SUCCESS", total_records)
        console.print("[bold green]Full load process finished successfully.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred during the full load process: {e}[/bold red]")
        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise typer.Exit(1)


from .acquisition import download_spl_archives
from .util import unzip_archive


@app.command()
def delta_load(ctx: typer.Context) -> None:
    """F008.3: Perform an incremental (delta) load from the FDA source."""
    settings: Settings = ctx.obj
    console.print("[bold cyan]Starting delta data load from FDA source...[/bold cyan]")
    loader = get_db_loader(settings)
    run_id = None
    downloaded_archives = []
    try:
        run_id = loader.start_run(mode="delta-load")

        console.print("[cyan]Step 1: Checking for and downloading new archives...[/cyan]")
        downloaded_archives = download_spl_archives(loader)
        if not downloaded_archives:
            console.print("[green]No new archives found. Database is up-to-date.[/green]")
            loader.end_run(run_id, "SUCCESS", 0)
            return
        console.print(f"[green]Downloaded {len(downloaded_archives)} new archive(s).[/green]")

        with tempfile.TemporaryDirectory() as xml_temp_dir_str, \
             tempfile.TemporaryDirectory() as intermediate_dir_str:

            xml_temp_dir = Path(xml_temp_dir_str)
            intermediate_dir = Path(intermediate_dir_str)
            writer = get_file_writer(settings, intermediate_dir)

            console.print(f"[cyan]Step 2: Extracting XML files to {xml_temp_dir}...[/cyan]")
            for archive in downloaded_archives:
                archive_path = Path(settings.download_path) / archive.name
                unzip_archive(archive_path, xml_temp_dir)

            console.print(f"[cyan]Step 3: Finding XML files in {xml_temp_dir}...[/cyan]")
            xml_files = list(xml_temp_dir.glob("**/*.xml"))
            console.print(f"Found {len(xml_files)} XML files to process.")

            console.print(
                f"[cyan]Step 4: Parsing and Transforming in parallel (max_workers={settings.max_workers})...[/cyan]"
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = _quarantine_and_parse_in_parallel(
                    xml_files, settings, executor
                )
                transformer = Transformer(writer=writer)
                stats = transformer.transform_stream(parsed_data_stream)
            console.print("[green]Parsing and Transformation complete.[/green]")

            console.print("[cyan]Step 5: Loading data into database...[/cyan]")
            loader.pre_load_optimization(mode="delta-load")
            loader.bulk_load_to_staging(intermediate_dir)
            loader.merge_from_staging("delta-load")
            loader.post_load_cleanup(mode="delta-load")
            console.print("[green]Database loading complete.[/green]")

            console.print("[cyan]Step 6: Recording processed archives in database...[/cyan]")
            for archive in downloaded_archives:
                loader.record_processed_archive(archive.name, archive.checksum)

        if run_id:
            total_records = sum(stats.values()) if stats else 0
            loader.end_run(run_id, "SUCCESS", total_records)
        console.print("[bold green]Delta load process finished successfully.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]An error occurred during the delta load process: {e}[/bold red]")
        logging.getLogger(__name__).exception("Delta load failed")
        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
