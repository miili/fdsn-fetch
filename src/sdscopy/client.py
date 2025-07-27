from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, NamedTuple

import aiohttp
from pydantic import BaseModel, ByteSize, Field, HttpUrl, PrivateAttr, computed_field
from rich.progress import Progress, TaskID

from sdscopy.models.station import Channel, Stations, parse_stations
from sdscopy.stats import Stats
from sdscopy.utils import NSL, human_readable_bytes

if TYPE_CHECKING:
    from rich.table import Table

    from sdscopy.writer import SDSWriter

logger = logging.getLogger(__name__)


def _clean_params(params: dict[str, Any]) -> None:
    """Remove empty values from the parameters dictionary."""
    for key in list(params.keys()):
        if not params[key]:
            del params[key]


class DownloadChannel(NamedTuple):
    """Data structure to hold download information."""

    channel: Channel
    date: date

    def sds_path(self, partial: bool = False) -> Path:
        """Return the SDS path for the channel on the specified date."""
        file_name = self.channel.sds_path(self.date)
        if partial:
            return file_name.parent / (file_name.name + ".partial")
        return file_name


class FDSNDownloadStats(BaseModel):
    time_start: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    download_speed: float = Field(
        default=0.0,
        description="Current download speed in bytes per second",
    )
    n_bytes: ByteSize = Field(
        default=ByteSize(0),
        description="Total number of bytes downloaded",
    )

    def add_chunk(self, n_bytes: int, elapsed_time: float) -> None:
        """Add a measurement to the download statistics."""
        self.n_bytes += n_bytes
        self.download_speed = n_bytes / elapsed_time if elapsed_time > 0 else 0.0


class FDSNClientStats(Stats):
    """Statistics for the FDSN client."""

    n_requests: int = Field(
        default=0,
        description="Number of requests made to the FDSN service",
    )
    n_bytes_downloaded: ByteSize = Field(
        default=ByteSize(0),
        description="Total number of bytes downloaded from the FDSN service",
    )
    downloads: list[FDSNDownloadStats] = Field(
        default_factory=list,
        description="List of download sessions with their statistics",
    )
    n_chunks_total: int = Field(
        default=0,
        description="Number of work items in the queue",
    )
    n_completed: int = Field(
        default=0,
        description="Number of completed downloads",
    )

    _station_work_count: defaultdict[NSL, int] = PrivateAttr(
        default_factory=lambda: defaultdict(int)
    )
    _client: FDSNClient | None = PrivateAttr(None)
    _progress: Progress = PrivateAttr(default_factory=Progress)
    _task_id: TaskID | None = PrivateAttr(None)

    def set_client(self, client: FDSNClient) -> None:
        """Set the FDSN client for this statistics instance."""
        self._client = client

    def start(self, n_work: int) -> None:
        """Start the progress tracking for downloads."""
        self.n_chunks_total = n_work
        self._task_id = self._progress.add_task(
            "Waiting...",
            total=n_work,
            visible=True,
            start=True,
        )

    def add_work(self, channel: DownloadChannel) -> None:
        """Add a work item to the statistics."""
        self._station_work_count[channel.channel.nsl] += 1
        self.n_chunks_total += 1

    def done_work(self, channel: DownloadChannel) -> None:
        """Mark a work item as done."""
        self._station_work_count[channel.channel.nsl] -= 1
        self.n_completed += 1

    @computed_field
    @property
    def n_stations(self) -> int:
        """Return the number of unique stations in the work queue."""
        return len(self._station_work_count)

    @computed_field
    @property
    def n_stations_completed(self) -> int:
        """Return the number of unique stations that have completed downloads."""
        return sum(1 for count in self._station_work_count.values() if count == 0)

    @computed_field
    @property
    def download_speed(self) -> ByteSize:
        """Calculate the average download speed across all downloads."""
        if not self.downloads:
            return ByteSize(0.0)
        return ByteSize(sum(download.download_speed for download in self.downloads))

    def _render(self, table: Table) -> None:
        """Render the statistics as a string."""
        if self._task_id is not None:
            self._progress.update(
                self._task_id,
                description=f"{human_readable_bytes(self.n_bytes_downloaded)}",
                completed=self.n_completed,
            )
        n_workers = len(self.downloads)
        table.add_row(
            "Server",
            f"[bold]{self._client.url if self._client else 'N/A'}[/bold] -"
            f" {n_workers} workers @ {self.download_speed.human_readable()}/s",
        )
        table.add_row(
            "Stations",
            f"{self.n_stations_completed} / {self.n_stations} completed",
        )
        table.add_row(
            "Progress",
            self._progress,
        )

    @asynccontextmanager
    async def download_stats(self):
        """Context manager to add a download session."""
        download = FDSNDownloadStats()
        self.downloads.append(download)
        try:
            yield download
        finally:
            self.downloads.remove(download)


class FDSNClient(BaseModel):
    url: HttpUrl = Field(
        default=HttpUrl("https://geofon.gfz.de"),
        description="Base URL of the FDSN web service",
    )
    timeout: float = Field(
        default=60.0,
        description="Timeout for HTTP requests in seconds",
    )
    max_connections: int = Field(
        default=8,
        description="Maximum number of concurrent connections",
    )

    available_stations: Stations = Field(
        default_factory=Stations,
        description="List of stations fetched from the FDSN service",
    )

    chunk_length: int = Field(
        default=4096,
        description="Length of data chunks to download in bytes",
    )

    _stats: FDSNClientStats = PrivateAttr(default_factory=FDSNClientStats)
    _client: aiohttp.ClientSession | None = PrivateAttr(None)

    _work_queue: asyncio.Queue[DownloadChannel] = PrivateAttr(
        default_factory=asyncio.Queue
    )

    async def prepare(
        self,
        selection: list[NSL],
        starttime: date,
        endtime: date,
    ) -> None:
        """Fetch available stations from the FDSN service."""
        self._stats.set_client(self)

        networks = {nsl.network for nsl in selection}
        stations = {nsl.station for nsl in selection}
        locations = {nsl.location for nsl in selection}

        params = {
            "network": ",".join(networks),
            "station": ",".join(stations),
            "location": ",".join(locations),
            "starttime": starttime.isoformat(),
            "endtime": endtime.isoformat(),
            "level": "channel",
            "format": "text",
            "nodata": "404",
        }
        _clean_params(params)

        logger.info("Preparing FDSN service: %s", self.url)

        async with (
            aiohttp.ClientSession(
                base_url=str(self.url),
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": "SDSCopyClient/1.0"},
            ) as client,
            client.get(
                "/fdsnws/station/1/query",
                params=params,
            ) as response,
        ):
            response.raise_for_status()
            data = await response.text()
            self.available_stations = parse_stations(data)

        logger.info(
            "Got %d stations from %s",
            self.available_stations.n_stations,
            self.url,
        )

    async def download_metadata(
        self,
        selection: list[NSL],
        starttime: date,
        endtime: date,
    ) -> str:
        """Fetch available stations from the FDSN service."""
        self._stats.set_client(self)

        networks = {nsl.network for nsl in selection}
        stations = {nsl.station for nsl in selection}
        locations = {nsl.location for nsl in selection}

        params = {
            "network": ",".join(networks),
            "station": ",".join(stations),
            "location": ",".join(locations),
            "starttime": starttime.isoformat(),
            "endtime": endtime.isoformat(),
            "level": "response",
            "format": "xml",
            "nodata": "404",
        }
        _clean_params(params)

        async with (
            aiohttp.ClientSession(
                base_url=str(self.url),
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": "SDSCopyClient/1.0"},
            ) as client,
            client.get(
                "/fdsnws/station/1/query",
                params=params,
            ) as response,
        ):
            response.raise_for_status()
            data = await response.text()

        logger.debug(
            "Downloaded metadata for %d stations from %s",
            len(selection),
            self.url,
        )
        return data

    async def download_waveform_data(
        self,
        client: aiohttp.ClientSession,
        station_channel: Channel,
        date: date,
    ) -> AsyncGenerator[bytes, None]:
        """Fetch waveform data for a specific channel on a given date."""
        params = {
            "network": station_channel.nsl.network,
            "station": station_channel.nsl.station,
            "location": station_channel.nsl.location,
            "channel": station_channel.code,
            "starttime": date.isoformat(),
            "endtime": (date + timedelta(days=1)).isoformat(),
            "format": "mseed",
            "nodata": "404",
        }
        _clean_params(params)
        async with (
            client.get(
                "/fdsnws/dataselect/1/query",
                params=params,
            ) as response,
            self._stats.download_stats() as download,
        ):
            self._stats.n_requests += 1
            response.raise_for_status()

            start_time = time.time()
            async for chunk in response.content.iter_chunked(self.chunk_length):
                yield chunk

                chunk_length = len(chunk)
                self._stats.n_bytes_downloaded += chunk_length
                download.add_chunk(chunk_length, time.time() - start_time)
                start_time = time.time()

    async def add_work(self, download: DownloadChannel) -> None:
        """Add a download task to the work queue."""
        await self._work_queue.put(download)
        self._stats.add_work(download)

    async def download(self, writer: SDSWriter) -> None:
        """Download data from the FDSN service."""
        if self._work_queue.empty():
            raise ValueError("No work available in the queue")

        self._stats.start(self._work_queue.qsize())

        async def worker(client: aiohttp.ClientSession) -> None:
            while not self._work_queue.empty():
                download = await self._work_queue.get()

                try:
                    async for data in self.download_waveform_data(
                        client=client,
                        station_channel=download.channel,
                        date=download.date,
                    ):
                        await writer.add_data(download, data)
                except TimeoutError:
                    logger.error(
                        "Timeout: Failed to download data for %s on %s",
                        download.channel.nsl.pretty,
                        download.date,
                    )
                    continue
                writer.done(download)
                self._stats.done_work(download)

                self._work_queue.task_done()

        logger.info(
            "Starting download from %s with %d workers",
            self.url,
            self.max_connections,
        )
        try:
            client = aiohttp.ClientSession(
                base_url=str(self.url),
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": "SDSCopyClient/1.0"},
            )
            async with asyncio.TaskGroup() as tg:
                for _ in range(self.max_connections):
                    tg.create_task(worker(client))
        finally:
            await client.close()

        logger.info("Download completed from %s", self.url)
