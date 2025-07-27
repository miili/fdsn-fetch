from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import rich
import typer
from rich.logging import RichHandler

from fdsn_download.manager import FDSNDownloadManager
from fdsn_download.stats import live_view

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)

app = typer.Typer()


@app.command()
def init():
    """Print the configuration."""
    client = FDSNDownloadManager()
    rich.print_json(client.model_dump_json())


@app.command()
def download(
    file: Annotated[
        Path,
        typer.Argument(
            ...,
            help="Path to the configuration file",
        ),
    ],
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    """Download data based on the provided configuration file."""
    client = FDSNDownloadManager.model_validate_json(file.read_text())

    log_level = logging.INFO
    if verbose >= 1:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    async def run_download() -> None:
        download = asyncio.create_task(client.download())
        stats_view = asyncio.create_task(live_view())
        await download
        stats_view.cancel()

    asyncio.run(run_download())


def main():
    """Main entry point for the SDSCopy application."""
    app()


if __name__ == "__main__":
    main()
