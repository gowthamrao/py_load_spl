import logging
import tempfile
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from . import __name__ as app_name
from .acquisition import download_all_archives
from .config import Settings, get_settings
from .main import get_db_loader, run_delta_load, run_full_load
from .util import setup_logging, unzip_archive

app = typer.Typer(name=app_name)
console = Console()
logger = logging.getLogger(__name__)


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
    setup_logging(log_level, log_format)
    settings = get_settings()
    if intermediate_format in ["csv", "parquet"]:
        settings.intermediate_format = intermediate_format  # type: ignore
    ctx.obj = settings
    if ctx.invoked_subcommand is None:
        console.print(
            "[bold red]No command specified. Use --help for options.[/bold red]"
        )


@app.command()
def init(ctx: typer.Context) -> None:
    """Initialize the database schema."""
    console.print("[bold green]Initializing database schema...[/bold green]")
    settings: Settings = ctx.obj
    try:
        loader = get_db_loader(settings)
        loader.initialize_schema()
        console.print("[bold green]Schema initialization complete.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Schema initialization failed: {e}[/bold red]")
        logger.exception("Schema initialization failed.")
        raise typer.Exit(1) from e


@app.command()
def full_load(
    ctx: typer.Context,
    source: Annotated[
        Path | None,
        typer.Option(
            help=(
                "Local path to SPL XML files. If not provided, all archives will be "
                "downloaded."
            ),
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
        ),
    ] = None,
) -> None:
    """Perform a full data load from a local directory or by downloading all archives."""
    settings: Settings = ctx.obj

    try:
        if source:
            if not source.exists():
                console.print(
                    f"[bold red]Error: Source path '{source}' does not exist.[/bold red]"
                )
                raise typer.Exit(1)
            run_full_load(settings, source)
        else:
            console.print(
                "[bold cyan]Step 0: No source path provided. Downloading all archives...[/bold cyan]"
            )
            with (
                tempfile.TemporaryDirectory() as download_dir,
                tempfile.TemporaryDirectory() as xml_dir,
            ):
                # Temporarily override download_path so archives go into our temp dir
                original_download_path = settings.download_path
                settings.download_path = download_dir
                try:
                    downloaded_archives = download_all_archives(settings)
                    if not downloaded_archives:
                        console.print(
                            "[bold yellow]No archives were downloaded. Nothing to do.[/bold yellow]"
                        )
                        return

                    xml_dir_path = Path(xml_dir)
                    console.print(
                        f"[cyan]Extracting {len(downloaded_archives)} archives to {xml_dir_path}...[/cyan]"
                    )
                    for archive in downloaded_archives:
                        archive_path = Path(settings.download_path) / archive.name
                        unzip_archive(archive_path, xml_dir_path)

                    run_full_load(settings, xml_dir_path)
                finally:
                    settings.download_path = original_download_path
        console.print(
            "[bold green]Full load process finished successfully.[/bold green]"
        )
    except Exception as e:
        console.print(
            f"[bold red]An error occurred during the full load process: {e}[/bold red]"
        )
        raise typer.Exit(1) from e


@app.command()
def delta_load(ctx: typer.Context) -> None:
    """Perform an incremental (delta) load from the FDA source."""
    settings: Settings = ctx.obj
    console.print("[bold cyan]Starting delta data load from FDA source...[/bold cyan]")
    try:
        run_delta_load(settings)
        console.print(
            "[bold green]Delta load process finished successfully.[/bold green]"
        )
    except Exception as e:
        console.print(
            f"[bold red]An error occurred during the delta load process: {e}[/bold red]"
        )
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
