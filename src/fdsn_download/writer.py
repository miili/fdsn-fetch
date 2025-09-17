from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field, PrivateAttr
from pyrocko.io import load, save
from pyrocko.trace import NoData
from rich.progress import track

from fdsn_download.client import DownloadDayfile
from fdsn_download.remote_log import RemoteLog
from fdsn_download.stats import Stats
from fdsn_download.utils import human_readable_bytes

if TYPE_CHECKING:
    from pyrocko.squirrel import Squirrel
    from pyrocko.trace import Trace
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

    steim_compression: Literal[1, 2] = Field(
        default=1,
        description="STEIM compression level (1 or 2)",
    )
    record_length: Literal[512, 4096] = Field(
        default=4096,
        description="Record length for MiniSEED files",
    )
    min_length_seconds: timedelta = Field(
        default=timedelta(minutes=1),
        description="Minimum length of data to be saved",
    )

    squirrel_environment: Path | None = Field(
        default=None,
        description="Path to the Squirrel environment, if applicable",
    )

    _stats: SDSWriterStats = PrivateAttr(default_factory=SDSWriterStats)
    _remote_log: RemoteLog = PrivateAttr(default_factory=RemoteLog)
    _squirrel: Squirrel | None = PrivateAttr(default=None)

    def has_chunk(self, channel: DownloadDayfile) -> bool:
        """Check if data for the given channel and date already exists."""
        file_path = self.sds_archive / channel.sds_path()
        return file_path.exists()

    @property
    def remote_log(self) -> RemoteLog:
        """Return the remote log for this writer."""
        return self._remote_log

    def get_squirrel(self) -> Squirrel:
        if self._squirrel is None:
            from pyrocko.squirrel import Squirrel

            self._squirrel = Squirrel(env=str(self.squirrel_environment))
        return self._squirrel

    async def add_data(self, channel: DownloadDayfile, data: bytes) -> None:
        """Write the downloaded data to the SDS path."""
        file_path = self.sds_archive / channel.sds_path(partial=True)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        async with _file_lock(file_path):
            with open(file_path, "ab") as file:
                await asyncio.to_thread(file.write, data)

    async def done(self, download: DownloadDayfile) -> None:
        """Finalize the download for the channel."""
        partial_file_path = self.sds_archive / download.sds_path(partial=True)
        final_file_path = self.sds_archive / download.sds_path()
        if not partial_file_path.exists():
            return

        try:
            traces: list[Trace] = load(str(partial_file_path))
        except Exception as e:
            logger.error("Failed to load MiniSEED file %s: %s", partial_file_path, e)
            return

        logger.debug("Trimming traces to dayfile time range")
        tmin, tmax = download.timestamp_range()
        trace_min_length = self.min_length_seconds.total_seconds()
        for trace in traces.copy():
            trace_length = trace.tmax - trace.tmin
            if trace_length < trace_min_length:
                logger.warning(
                    "Removing short trace from %s.%s of %.1f s on %s",
                    download.channel.nsl.pretty,
                    download.channel.code,
                    trace_length,
                    download.date,
                )
                traces.remove(trace)
                continue
            try:
                await asyncio.to_thread(trace.chop, tmin=tmin, tmax=tmax)
            except NoData:
                continue

        logger.debug(
            "Saving traces with STEIM%d compression and record length %d",
            self.steim_compression,
            self.record_length,
        )
        with NamedTemporaryFile() as tmp_file:
            await asyncio.to_thread(
                save,
                traces,
                tmp_file.name,
                format="mseed",
                steim=self.steim_compression,
                record_length=self.record_length,
                check_overlaps=False,
            )
            tmp_file.flush()
            tmp_file_path = Path(tmp_file.name)
            tmp_file_path.replace(final_file_path)

        partial_file_path.unlink()
        file_size = final_file_path.stat().st_size

        self._stats.total_files_saved += 1
        self._stats.total_bytes_written += file_size
        self._stats.archive_size += file_size

        logger.info("Saved %s", final_file_path)

        if self.squirrel_environment is not None:
            squirrel = self.get_squirrel()
            await asyncio.to_thread(squirrel.add, str(final_file_path))

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
