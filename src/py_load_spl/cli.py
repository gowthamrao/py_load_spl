import logging
import tempfile
from pathlib import Path

import typer
from rich.console import Console

from . import __name__ as app_name
from .config import get_settings
from .db.postgres import PostgresLoader
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
        ..., help="Local path to the directory containing SPL XML files.", exists=True, file_okay=False, dir_okay=True, readable=True
    ),
) -> None:
    """
    F008.3: Perform a full data load from a local directory.
    """
    console.print(f"[bold cyan]Starting full data load from '{source}'...[/bold cyan]")

    with tempfile.TemporaryDirectory() as temp_dir_str:
        output_dir = Path(temp_dir_str)
        console.print(f"Intermediate CSV files will be stored in: {output_dir}")

        # 1. Parsing
        console.print("[cyan]Step 1: Parsing XML files...[/cyan]")
        parsed_data_stream = iter_spl_files(source)

        # 2. Transformation
        console.print("[cyan]Step 2: Transforming data to CSV...[/cyan]")
        transformer = Transformer(output_dir=output_dir)
        transformer.transform_stream(parsed_data_stream)

        console.print("[green]Parsing and Transformation complete.[/green]")

        # 3. Loading (The next step in the implementation)
        console.print("[yellow]Step 3: Loading data to database (not yet implemented).[/yellow]")
        # TODO: Instantiate the DB loader
        # TODO: Call loader.bulk_load_to_staging(output_dir)
        # TODO: Call loader.merge_from_staging('full-load')
        # etc.

    console.print("[bold green]Full load process finished.[/bold green]")


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
    console.print("[bold blue]Delta data load complete.[/bold blue]")


if __name__ == "__main__":
    app()
