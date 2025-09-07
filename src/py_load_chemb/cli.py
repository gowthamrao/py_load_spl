import logging

import typer
from rich.console import Console

from . import __name__ as app_name

app = typer.Typer(name=app_name)
console = Console()


@app.callback()
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


@app.command()
def init() -> None:
    """
    F008.3: Initialize the database schema.
    """
    console.print("[bold green]Initializing database schema...[/bold green]")
    # TODO:
    # 1. Get settings
    # 2. Get DB adapter from settings
    # 3. Call adapter.initialize_schema()
    console.print("[bold green]Schema initialization complete.[/bold green]")


@app.command()
def full_load(
    ctx: typer.Context,
    source: str | None = typer.Option(
        None, help="Local path to SPL archives instead of downloading."
    ),
) -> None:
    """
    F008.3: Perform a full data load.
    Downloads all SPL data, truncates tables, and reloads everything.
    """
    console.print("[bold cyan]Starting full data load...[/bold cyan]")
    # TODO:
    # 1. Acquisition
    # 2. Parsing
    # 3. Transformation
    # 4. Loading (using pre_load, bulk_load, merge, post_load)
    console.print("[bold cyan]Full data load complete.[/bold cyan]")


@app.command()
def delta_load(
    ctx: typer.Context,
    source: str | None = typer.Option(
        None, help="Local path to SPL archives instead of downloading."
    ),
) -> None:
    """
    F008.3: Perform an incremental (delta) load.
    Downloads only new/updated SPL data and merges it into the database.
    """
    console.print("[bold blue]Starting delta data load...[/bold blue]")
    # TODO:
    # 1. Acquisition (with delta identification)
    # 2. Parsing
    # 3. Transformation
    # 4. Loading (using merge/upsert)
    console.print("[bold blue]Delta data load complete.[/bold blue]")


if __name__ == "__main__":
    app()
