import logging
import tempfile
import zipfile
from pathlib import Path
from typing import List

import typer
from rich.console import Console

from . import __name__ as app_name
from .acquisition import Archive
from .config import get_settings
from .parsing import iter_spl_files
from .transformation import Transformer

app = typer.Typer(name=app_name)
console = Console()


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    log_level: str = typer.Option(
        "INFO", help="Set the logging level.", envvar="LOG_LEVEL"
    ),
) -> None:
    """
    A CLI for the SPL Data Loader.
    """
    # Setup logging
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logging.getLogger(__name__).info(f"Logging level set to {log_level}")

    # Load settings and store them in the context for other commands to use
    ctx.obj = get_settings()

    if ctx.invoked_subcommand is None:
        console.print("[bold red]No command specified. Use --help for options.[/bold red]")


from .acquisition import download_spl_archives


@app.command()
def download(ctx: typer.Context) -> None:
    """
    F001: Download SPL archives from the FDA source.
    This command fetches the list of available SPL data archives and
    downloads a sample file to demonstrate the functionality.
    """
    console.print("[bold green]Starting data acquisition process...[/bold green]")
    try:
        download_spl_archives()
        console.print("[bold green]Data acquisition command finished.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred during data acquisition: {e}[/bold red]")
        raise typer.Exit(1)


@app.command()
def init(ctx: typer.Context) -> None:
    """
    F008.3: Initialize the database schema.
    """
    from .db.postgres import PostgresLoader

    console.print("[bold green]Initializing database schema...[/bold green]")
    settings = ctx.obj
    # This is a simple factory, could be expanded for other adapters
    if settings.db.adapter == "postgresql":
        loader = PostgresLoader(settings.db)
    else:
        console.print(f"[bold red]Error: Unsupported DB adapter '{settings.db.adapter}'[/bold red]")
        raise typer.Exit(1)

    try:
        loader.initialize_schema()
        console.print("[bold green]Schema initialization complete.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Schema initialization failed: {e}[/bold red]")
        raise typer.Exit(1)


from .db.postgres import PostgresLoader


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
    """
    F008.3: Perform a full data load from a local directory.
    This command orchestrates the E-T-L process:
    - Extracts and parses XML files from the source directory.
    - Transforms the data into intermediate CSV files.
    - Loads the CSV files into a PostgreSQL database.
    """
    settings = ctx.obj
    console.print(f"[bold cyan]Starting full data load from '{source}'...[/bold cyan]")

    # Initialize the correct database loader
    if settings.db.adapter == "postgresql":
        loader = PostgresLoader(settings.db)
    else:
        console.print(f"[bold red]Error: Unsupported DB adapter '{settings.db.adapter}'[/bold red]")
        raise typer.Exit(1)

    run_id = None
    try:
        # Start a new run record in the ETL history
        run_id = loader.start_run(mode="full-load")

        with tempfile.TemporaryDirectory() as temp_dir_str:
            output_dir = Path(temp_dir_str)
            console.print(f"Intermediate CSV files will be stored in: {output_dir}")

            # 1. Parsing
            console.print("[cyan]Step 1: Parsing and Transforming...[/cyan]")
            parsed_data_stream = iter_spl_files(source)
            transformer = Transformer(output_dir=output_dir)
            transformer.transform_stream(parsed_data_stream)
            console.print("[green]Parsing and Transformation complete.[/green]")

            # 2. Loading
            console.print("[cyan]Step 2: Loading data into database...[/cyan]")
            loader.pre_load_optimization()
            loader.bulk_load_to_staging(output_dir)
            loader.merge_from_staging("full-load")
            loader.post_load_cleanup()
            console.print("[green]Database loading complete.[/green]")

        # If all steps succeeded, update the run status to SUCCESS
        if run_id:
            # In a real implementation, we would count records loaded.
            # For now, we'll use a placeholder.
            loader.end_run(run_id, "SUCCESS", records_loaded=0)
        console.print("[bold green]Full load process finished successfully.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]An error occurred during the full load process: {e}[/bold red]")
        # If an error occurred, update the run status to FAILED
        if run_id:
            loader.end_run(run_id, "FAILED", error_log=str(e))
        raise typer.Exit(1)


@app.command()
def delta_load(ctx: typer.Context) -> None:
    """
    F008.3: Perform an incremental (delta) load.
    Downloads new SPL archives, processes them one by one, and loads the data
    into the database using an UPSERT strategy.
    """
    settings = ctx.obj
    console.print("[bold blue]Starting delta data load...[/bold blue]")
    run_id = None
    try:
        # 1. Acquisition: Find and download new archives
        console.print("[cyan]Step 1: Acquiring new archives...[/cyan]")
        newly_downloaded_archives: List[Archive] = download_spl_archives()
        if not newly_downloaded_archives:
            console.print("[green]No new archives to process. Database is up to date.[/green]")
            return

        # Initialize the loader once
        loader = PostgresLoader(settings.db)
        run_id = loader.start_run(mode="delta-load")
        total_archives_processed = 0

        # Process each archive individually
        for archive in newly_downloaded_archives:
            console.print(f"--> Processing archive: [bold]{archive['name']}[/bold]")
            try:
                with tempfile.TemporaryDirectory() as xml_dir_str, tempfile.TemporaryDirectory() as csv_dir_str:
                    xml_dir = Path(xml_dir_str)
                    csv_dir = Path(csv_dir_str)

                    # 2. Unzip archive
                    console.print(f"    Unzipping {archive['name']}...")
                    download_path = Path(settings.download_path) / archive["name"]
                    with zipfile.ZipFile(download_path, "r") as zip_ref:
                        zip_ref.extractall(xml_dir)

                    # 3. Transform
                    console.print("    Parsing and Transforming XML to CSV...")
                    parsed_stream = iter_spl_files(xml_dir)
                    transformer = Transformer(output_dir=csv_dir)
                    transformer.transform_stream(parsed_stream)

                    # 4. Load
                    console.print("    Loading data into database...")
                    loader.bulk_load_to_staging(csv_dir)
                    loader.merge_from_staging(mode="delta-load")

                    # 5. Record success for this archive
                    loader.record_processed_archive(archive["name"], archive["checksum"])
                    console.print(f"    [green]Successfully processed {archive['name']}[/green]")
                    total_archives_processed += 1

            except Exception as e:
                console.print(f"    [bold red]Failed to process archive {archive['name']}. Error: {e}[/bold red]")
                # Continue to the next archive
                continue

        loader.end_run(run_id, "SUCCESS", archives_processed=total_archives_processed)
        console.print("[bold green]Delta load process finished successfully.[/bold green]")

    except Exception as e:
        console.print(f"[bold red]An error occurred during the delta load process: {e}[/bold red]")
        if run_id:
            # This assumes loader was initialized
            PostgresLoader(settings.db).end_run(run_id, "FAILED", error_log=str(e))
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
