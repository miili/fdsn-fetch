from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr
from rich.progress import track

from fdsn_download.client import DownloadChunk
from fdsn_download.remote_log import RemoteLog
from fdsn_download.stats import Stats
from fdsn_download.utils import human_readable_bytes

if TYPE_CHECKING:
    from pyrocko.squirrel import Squirrel
    from rich.table import Table


logger = logging.getLogger(__name__)


@dataclass
class FileLock:
    """A named tuple to hold file lock information."""

    accessors: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


FILE_LOCKS = defaultdict(FileLock)


@asynccontextmanager
async def _file_lock(file_path: Path):
    """Context manager to handle file locks."""
    file_lock = FILE_LOCKS[file_path]
    file_lock.accessors += 1
    async with file_lock.lock:
        yield
    file_lock.accessors -= 1
    if file_lock.accessors == 0:
        FILE_LOCKS.pop(file_path, None)


class SDSWriterStats(Stats):
    _pos: int = 10

    total_files_saved: int = Field(
        default=0,
        title="Total Files Written",
        description="Total number of files written to disk",
    )
    total_bytes_written: int = Field(
        default=0,
        title="Total Bytes Written",
        description="Total number of bytes written to disk",
    )
    archive_size: int = Field(
        default=0,
        title="Archive Size",
        description="Total size of the archive in bytes",
    )

    def _render(self, table: Table) -> None:
        """Render the statistics as a string."""
        table.add_row(
            "SDS Archive",
            human_readable_bytes(self.archive_size),
            style="dim",
        )
        table.add_row(
            "Dayfiles saved",
            f"{self.total_files_saved} ({human_readable_bytes(self.total_bytes_written)})",
            style="dim",
        )


class SDSWriter(BaseModel):
    sds_archive: Path = Field(
        default=Path("./data/"),
        description="Base path for storing SDS data",
    )

    squirrel_environment: Path | None = Field(
        default=None,
        description="Path to the Squirrel environment, if applicable",
    )

    write_theads: int = Field(
        default=4,
        ge=1,
        description="Number of threads to use for writing SDS files",
    )

    _stats: SDSWriterStats = PrivateAttr(default_factory=SDSWriterStats)
    _consume_queue: asyncio.Queue[tuple[DownloadChunk, bytes | None]] = PrivateAttr(
        default_factory=asyncio.Queue
    )
    _remote_log: RemoteLog = PrivateAttr(default_factory=RemoteLog)
    _squirrel: Squirrel | None = PrivateAttr(default=None)
    _executor: ThreadPoolExecutor = PrivateAttr(
        default_factory=lambda: ThreadPoolExecutor(
            max_workers=4,
            thread_name_prefix="SDSWriterExecutor",
        )
    )

    def has_chunk(self, channel: DownloadChunk) -> bool:
        """Check if data for the given channel and date already exists."""
        file_path = self.sds_archive / channel.sds_path()
        return file_path.exists()

    @property
    def remote_log(self) -> RemoteLog:
        """Return the remote log for this writer."""
        return self._remote_log

    def add_data(self, channel: DownloadChunk, data: bytes) -> None:
        """Add a channel to the download queue."""
        self._consume_queue.put_nowait((channel, data))

    def done(self, channel: DownloadChunk) -> None:
        """Mark the download for the channel as done."""
        self._consume_queue.put_nowait((channel, None))

    def get_squirrel(self) -> Squirrel:
        if self._squirrel is None:
            from pyrocko.squirrel import Squirrel

            self._squirrel = Squirrel(env=str(self.squirrel_environment))
        return self._squirrel

    async def _add_data(self, channel: DownloadChunk, data: bytes) -> None:
        """Write the downloaded data to the SDS path."""
        file_path = self.sds_archive / channel.sds_path(partial=True)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_running_loop()
        async with _file_lock(file_path):
            with open(file_path, "ab") as file:
                await loop.run_in_executor(self._executor, file.write, data)

    async def _done(self, chunk: DownloadChunk) -> None:
        """Finalize the download for the channel."""
        partial_file_path = self.sds_archive / chunk.sds_path(partial=True)
        if not partial_file_path.exists():
            return

        file_path = self.sds_archive / chunk.sds_path()
        partial_file_path.rename(file_path)
        file_size = file_path.stat().st_size

        self._stats.total_files_saved += 1
        self._stats.total_bytes_written += file_size
        self._stats.archive_size += file_size

        logger.info("Saved %s", file_path)

        if self.squirrel_environment is not None:
            squirrel = self.get_squirrel()
            await asyncio.to_thread(squirrel.add, str(file_path))

    async def start(self):
        logger.info("Starting SDSWriter with %d threads", self.write_theads)

        async def write_worker(worker_id: int):
            try:
                while True:
                    channel, data = await self._consume_queue.get()
                    if data is None:
                        await self._done(channel)
                    else:
                        await self._add_data(channel, data)
                    self._consume_queue.task_done()
            except asyncio.CancelledError:
                logger.debug("SDS Worker %d stopped", worker_id)

        async with asyncio.TaskGroup() as tg:
            for i_worker in range(self.write_theads):
                tg.create_task(write_worker(i_worker))

    async def prepare(self) -> None:
        """Remove any partial files that may exist."""
        logger.debug("Cleaning up partial files in %s", self.sds_archive)
        for file in self.sds_archive.glob("**/*.partial"):
            file.unlink()
            logger.warning("Removed partial file %s", file)

        for i_file, file in track(
            enumerate(self.sds_archive.glob("**/*.[0-9]*")),
            description="Scanning archive...",
            show_speed=False,
        ):
            file_size = file.stat().st_size
            if file_size == 0:
                file.unlink()
                logger.warning("Removed empty file %s", file)
                continue

            self._stats.archive_size += file_size

            if i_file % 1000 == 0:
                await asyncio.sleep(0.0)

        remote_log = self.sds_archive / "remote_errors.log"
        self._remote_log.set_logfile(remote_log)

        self._executor = ThreadPoolExecutor(
            max_workers=self.write_theads,
            thread_name_prefix="SDSWriterExecutor",
        )
