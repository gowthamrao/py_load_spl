import logging

import typer
from rich.console import Console

from . import __name__ as app_name
from .config import get_settings
from .db.postgres import PostgresLoader

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
    A CLI for the FAERS Data Loader.
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


import typing
from pathlib import Path

from . import acquisition


@app.command()
def download(
    ctx: typer.Context,
    quarter: typing.Optional[str] = typer.Argument(
        None, help="The quarter to download in YYYYQ# format (e.g., 2025Q1)."
    ),
    file_format: str = typer.Option(
        "ascii", "--format", help="The file format to download ('ascii' or 'xml')."
    ),
    latest: bool = typer.Option(
        False, "--latest", help="Download the most recent available quarter."
    ),
    output_dir: Path = typer.Option(
        "data/raw",
        "--out",
        help="The directory to save the downloaded file.",
        writable=True,
    ),
) -> None:
    """
    Download FAERS quarterly data files from the FDA website.
    """
    if not quarter and not latest:
        console.print("[bold red]Error: You must specify a quarter or use --latest.[/bold red]")
        raise typer.Exit(1)

    if quarter and latest:
        console.print("[bold red]Error: You cannot specify both a quarter and --latest.[/bold red]")
        raise typer.Exit(1)

    console.print("[bold cyan]Finding available FAERS archives...[/bold cyan]")
    archives = list(acquisition.list_available_archives())

    # Filter for the correct format first
    archives = [a for a in archives if a["format"] == file_format.lower()]

    if not archives:
        console.print(f"[bold red]No archives found for format '{file_format}'.[/bold red]")
        raise typer.Exit(1)

    target_archive = None
    if latest:
        # Sort by year, then quarter to find the latest
        target_archive = sorted(archives, key=lambda x: x["quarter"], reverse=True)[0]
        console.print(f"Found latest quarter: [bold yellow]{target_archive['quarter']}[/bold yellow]")
    else:
        for archive in archives:
            if archive["quarter"].lower() == quarter.lower():
                target_archive = archive
                break
        if not target_archive:
            console.print(f"[bold red]Error: Quarter '{quarter}' not found for format '{file_format}'.[/bold red]")
            raise typer.Exit(1)

    url = target_archive["url"]
    filename = url.split("/")[-1]
    destination = output_dir / filename

    if destination.exists():
        console.print(f"[yellow]File {destination} already exists. Skipping download.[/yellow]")
        # Optionally, add a --force flag to re-download
    else:
        try:
            result = acquisition.download_archive(url, destination)
            console.print("\n[bold green]Download successful![/bold green]")
            console.print(f"  Path: {result['path']}")
            console.print(f"  SHA256: {result['sha256']}")
        except Exception as e:
            console.print(f"\n[bold red]Download failed: {e}[/bold red]")
            raise typer.Exit(1)


from . import parsing


@app.command()
def parse(
    ctx: typer.Context,
    source_dir: Path = typer.Argument(
        ..., help="Directory containing the extracted FAERS ASCII .txt files.",
        exists=True, file_okay=False, resolve_path=True,
    ),
    staging_dir: Path = typer.Option(
        "data/processed",
        "--out",
        help="The directory to save the staged CSV files.",
        writable=True, resolve_path=True,
    ),
) -> None:
    """
    Parse FAERS ASCII data and stage it into clean CSV files.
    """
    console.print(f"[bold cyan]Parsing ASCII files from '{source_dir}'...[/bold cyan]")
    try:
        counts = parsing.parse_and_stage_ascii_quarter(source_dir, staging_dir)
        console.print("\n[bold green]Parsing and staging successful![/bold green]")
        for table, count in counts.items():
            console.print(f"  - Wrote {count} rows for [yellow]{table}[/yellow] to {staging_dir}")
    except Exception as e:
        console.print(f"\n[bold red]Parsing failed: {e}[/bold red]")
        raise typer.Exit(1)


@app.command()
def init_db(ctx: typer.Context) -> None:
    """
    Initialize the database schema.
    """
    # TODO: This command will fail until a new FAERS-specific schema is created.
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


if __name__ == "__main__":
    app()
