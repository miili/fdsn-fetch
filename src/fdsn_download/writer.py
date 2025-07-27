from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr

from fdsn_download.client import DownloadChannel

if TYPE_CHECKING:
    from pyrocko.squirrel import Squirrel


logger = logging.getLogger(__name__)


class SDSWriter(BaseModel):
    base_path: Path = Field(
        default=Path("./data/"),
        description="Base path for storing SDS data",
    )

    squirrel_environment: Path | None = Field(
        default=None,
        description="Path to the Squirrel environment, if applicable",
    )

    _consume_queue: asyncio.Queue[tuple[DownloadChannel, bytes | None]] = PrivateAttr(
        default_factory=asyncio.Queue
    )
    _squirrel: Squirrel | None = PrivateAttr(default=None)

    def has_channel(self, channel: DownloadChannel) -> bool:
        """Check if data for the given channel and date already exists."""
        file_path = self.base_path / channel.sds_path()
        return file_path.exists()

    def add_data(self, channel: DownloadChannel, data: bytes) -> None:
        """Add a channel to the download queue."""
        self._consume_queue.put_nowait((channel, data))

    def done(self, channel: DownloadChannel) -> None:
        """Mark the download for the channel as done."""
        self._consume_queue.put_nowait((channel, None))

    def get_squirrel(self) -> Squirrel:
        if self._squirrel is None:
            from pyrocko.squirrel import Squirrel

            self._squirrel = Squirrel(env=str(self.squirrel_environment))
        return self._squirrel

    async def _add_data(self, channel: DownloadChannel, data: bytes) -> None:
        """Write the downloaded data to the SDS path."""
        file_path = self.base_path / channel.sds_path(partial=True)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "ab") as file:
            await asyncio.to_thread(file.write, data)

    def _done(self, channel: DownloadChannel) -> None:
        """Finalize the download for the channel."""
        partial_file_path = self.base_path / channel.sds_path(partial=True)
        if not partial_file_path.exists():
            return

        file_path = self.base_path / channel.sds_path()
        partial_file_path.rename(file_path)
        logger.info("Downloaded %s", file_path)

        if self.squirrel_environment is not None:
            squirrel = self.get_squirrel()
            squirrel.add(str(file_path))

    async def start(self):
        try:
            while True:
                channel, data = await self._consume_queue.get()
                if data is None:
                    self._done(channel)
                else:
                    await self._add_data(channel, data)
                self._consume_queue.task_done()
        except asyncio.CancelledError:
            logger.info("SDSWriter stopped.")

    def clean_partial_files(self) -> None:
        """Remove any partial files that may exist."""
        logger.debug("Cleaning up partial files in %s", self.base_path)
        for file in self.base_path.glob("**/*.partial"):
            file.unlink()
            logger.warning("Removed partial file %s", file)
