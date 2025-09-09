import concurrent.futures
import logging
import tempfile
from pathlib import Path

import typer
from rich.console import Console

from . import __name__ as app_name
from .config import get_settings
from .db.base import DatabaseLoader
from .db.postgres import PostgresLoader
from .parsing import parse_spl_file
from .transformation import Transformer
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
) -> None:
    """A CLI for the SPL Data Loader."""
    setup_logging(log_level, log_format)
    ctx.obj = get_settings()
    if ctx.invoked_subcommand is None:
        console.print("[bold red]No command specified. Use --help for options.[/bold red]")


def get_db_loader(settings) -> DatabaseLoader:
    if settings.db.adapter == "postgresql":
        return PostgresLoader(settings.db)
    else:
        console.print(f"[bold red]Error: Unsupported DB adapter '{settings.db.adapter}'[/bold red]")
        raise typer.Exit(1)


@app.command()
def init(ctx: typer.Context) -> None:
    """F008.3: Initialize the database schema."""
    console.print("[bold green]Initializing database schema...[/bold green]")
    settings = ctx.obj
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
    settings = ctx.obj
    console.print(f"[bold cyan]Starting full data load from '{source}'...[/bold cyan]")
    loader = get_db_loader(settings)
    run_id = None
    try:
        run_id = loader.start_run(mode="full-load")
        with tempfile.TemporaryDirectory() as temp_dir_str:
            output_dir = Path(temp_dir_str)
            console.print(f"Intermediate CSV files will be stored in: {output_dir}")

            console.print("[cyan]Step 1: Finding XML files...[/cyan]")
            xml_files = list(source.glob("**/*.xml"))
            console.print(f"Found {len(xml_files)} XML files to process.")

            console.print(
                f"[cyan]Step 2: Parsing and Transforming in parallel (max_workers={settings.max_workers})...[/cyan]"
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = executor.map(parse_spl_file, xml_files)
                transformer = Transformer(output_dir=output_dir)
                # The 'stats' variable will be used in the next step of the plan
                stats = transformer.transform_stream(parsed_data_stream)

            console.print("[green]Parsing and Transformation complete.[/green]")

            console.print("[cyan]Step 3: Loading data into database...[/cyan]")
            loader.pre_load_optimization()
            loader.bulk_load_to_staging(output_dir)
            loader.merge_from_staging("full-load")
            loader.post_load_cleanup()
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
    settings = ctx.obj
    console.print("[bold cyan]Starting delta data load from FDA source...[/bold cyan]")
    loader = get_db_loader(settings)
    run_id = None
    downloaded_archives = []
    try:
        run_id = loader.start_run(mode="delta-load")

        # Step 1: Download new archives
        console.print("[cyan]Step 1: Checking for and downloading new archives...[/cyan]")
        downloaded_archives = download_spl_archives(loader)
        if not downloaded_archives:
            console.print("[green]No new archives found. Database is up-to-date.[/green]")
            loader.end_run(run_id, "SUCCESS", 0)
            return

        console.print(f"[green]Downloaded {len(downloaded_archives)} new archive(s).[/green]")

        # Create temporary directories for processing
        with tempfile.TemporaryDirectory() as xml_temp_dir_str, \
             tempfile.TemporaryDirectory() as csv_temp_dir_str:

            xml_temp_dir = Path(xml_temp_dir_str)
            csv_temp_dir = Path(csv_temp_dir_str)

            # Step 2: Unzip all archives
            console.print(f"[cyan]Step 2: Extracting XML files to {xml_temp_dir}...[/cyan]")
            for archive in downloaded_archives:
                archive_path = Path(settings.download_path) / archive.name
                unzip_archive(archive_path, xml_temp_dir)

            # Step 3: Transform XMLs to CSVs
            console.print(f"[cyan]Step 3: Finding XML files in {xml_temp_dir}...[/cyan]")
            xml_files = list(xml_temp_dir.glob("**/*.xml"))
            console.print(f"Found {len(xml_files)} XML files to process.")

            console.print(
                f"[cyan]Step 4: Parsing and Transforming in parallel (max_workers={settings.max_workers})...[/cyan]"
            )
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=settings.max_workers
            ) as executor:
                parsed_data_stream = executor.map(parse_spl_file, xml_files)
                transformer = Transformer(output_dir=csv_temp_dir)
                stats = transformer.transform_stream(parsed_data_stream)
            console.print("[green]Parsing and Transformation complete.[/green]")

            # Step 5: Load data into database
            console.print("[cyan]Step 5: Loading data into database...[/cyan]")
            loader.pre_load_optimization()
            loader.bulk_load_to_staging(csv_temp_dir)
            loader.merge_from_staging("delta-load")
            loader.post_load_cleanup()
            console.print("[green]Database loading complete.[/green]")

            # Step 6: Record processed archives
            console.print("[cyan]Step 6: Recording processed archives in database...[/cyan]")
            for archive in downloaded_archives:
                loader.record_processed_archive(archive.name, archive.checksum)

        if run_id:
            # Use the stats from the transformer for a more accurate count
            total_records = sum(stats.values()) if stats else 0
            loader.end_run(run_id, "SUCCESS", total_records)
        console.print("[bold green]Delta load process finished successfully.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]An error occurred during the delta load process: {e}[/bold red]")
        # Also log the traceback for debugging
        logging.getLogger(__name__).exception("Delta load failed")
        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
