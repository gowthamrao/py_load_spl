import logging
import tempfile
from pathlib import Path

import typer
from rich.console import Console

from . import __name__ as app_name
from .config import get_settings
from .db.base import DatabaseLoader
from .db.postgres import PostgresLoader
from .parsing import iter_spl_files
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

            console.print("[cyan]Step 1: Parsing and Transforming...[/cyan]")
            parsed_data_stream = iter_spl_files(source)
            transformer = Transformer(output_dir=output_dir)
            transformer.transform_stream(parsed_data_stream)
            console.print("[green]Parsing and Transformation complete.[/green]")

            console.print("[cyan]Step 2: Loading data into database...[/cyan]")
            loader.pre_load_optimization()
            loader.bulk_load_to_staging(output_dir)
            loader.merge_from_staging("full-load")
            loader.post_load_cleanup()
            console.print("[green]Database loading complete.[/green]")

        if run_id:
            loader.end_run(run_id, "SUCCESS", records_loaded=-1) # Placeholder
        console.print("[bold green]Full load process finished successfully.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]An error occurred during the full load process: {e}[/bold red]")
        if run_id:
            loader.end_run(run_id, "FAILED", 0, str(e))
        raise typer.Exit(1)


@app.command()
def delta_load(ctx: typer.Context) -> None:
    """F008.3: Perform an incremental (delta) load. (Not Implemented)"""
    console.print("[bold yellow]Delta load is not yet implemented.[/bold yellow]")


if __name__ == "__main__":
    app()
