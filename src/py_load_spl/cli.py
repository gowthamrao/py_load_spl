import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.pretty import pprint

from . import __name__ as app_name
from .config import get_settings
from .db.postgres import PostgresLoader
from .parsing import iter_spl_files

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


@app.command()
def init(ctx: typer.Context) -> None:
    """
    F008.3: Initialize the database schema.
    """
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


@app.command()
def full_load(
    ctx: typer.Context,
    source: Path = typer.Option(
        "data", help="Local path to SPL archives instead of downloading."
    ),
) -> None:
    """
    F008.3: Perform a full data load.
    (Currently only demonstrates parsing from a local directory)
    """
    console.print(f"[bold cyan]Starting full data load from '{source}'...[/bold cyan]")

    # In a full implementation, this would be followed by transformation and loading.
    # For now, we just demonstrate the parsing.
    record_count = 0
    for parsed_data in iter_spl_files(source):
        console.print(f"--- Parsed Record (doc_id: {parsed_data.get('document_id')}) ---")
        pprint(parsed_data)
        record_count += 1

    console.print(f"[bold cyan]Parsing complete. Found {record_count} records.[/bold cyan]")


@app.command()
def delta_load(
    ctx: typer.Context,
    source: str | None = typer.Option(
        None, help="Local path to SPL archives instead of downloading."
    ),
) -> None:
    """
    F008.3: Perform an incremental (delta) load.
    (Not yet implemented)
    """
    console.print("[bold blue]Starting delta data load...[/bold blue]")
    console.print("[yellow]Note: Delta load is not yet implemented.[/yellow]")
    # TODO:
    # 1. Acquisition (with delta identification)
    # 2. Parsing
    # 3. Transformation
    # 4. Loading (using merge/upsert)
    console.print("[bold blue]Delta data load complete.[/bold blue]")


if __name__ == "__main__":
    app()
