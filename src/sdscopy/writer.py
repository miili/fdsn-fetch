from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from pydantic import BaseModel, Field

from sdscopy.client import DownloadChannel

logger = logging.getLogger(__name__)


class SDSWriter(BaseModel):
    base_path: Path = Field(
        default=Path("./data/"),
        description="Base path for storing SDS data",
    )

    def has_channel(self, channel: DownloadChannel) -> bool:
        """Check if data for the given channel and date already exists."""
        file_path = self.base_path / channel.sds_path()
        return file_path.exists()

    def done(self, channel: DownloadChannel) -> None:
        """Finalize the download for the channel."""
        partial_file_path = self.base_path / channel.sds_path(partial=True)
        if not partial_file_path.exists():
            return

        file_path = self.base_path / channel.sds_path()
        partial_file_path.rename(file_path)
        logger.info("Downloaded %s", file_path)

    async def add_data(self, channel: DownloadChannel, data: bytes) -> None:
        """Write the downloaded data to the SDS path."""
        file_path = self.base_path / channel.sds_path(partial=True)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "ab") as file:
            await asyncio.to_thread(file.write, data)

    def clean_partial_files(self) -> None:
        """Remove any partial files that may exist."""
        logger.debug("Cleaning up partial files in %s", self.base_path)
        for file in self.base_path.glob("**/*.partial"):
            file.unlink()
            logger.warning("Removed partial file %s", file)
