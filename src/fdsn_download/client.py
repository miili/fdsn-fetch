from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator

import aiohttp
from pydantic import BaseModel, ByteSize, Field, HttpUrl, PrivateAttr, computed_field
from rich.progress import Progress, TaskID

from fdsn_download.models.station import Channel, Stations, parse_stations
from fdsn_download.stats import Stats
from fdsn_download.utils import (
    NSL,
    ByteSizeStr,
    datetime_now,
    human_readable_bytes,
)

if TYPE_CHECKING:
    from rich.table import Table

    from fdsn_download.writer import SDSWriter

logger = logging.getLogger(__name__)

MB = 1024 * 1024
HEADERS = {"User-Agent": "FDSN-Download-Client/1.0"}


def _clean_params(params: dict[str, Any]) -> None:
    """Remove empty values from the parameters dictionary."""
    for key in list(params.keys()):
        if not params[key]:
            params.pop(key, None)


def get_error_str(error_code: int) -> str:
    """Return a human-readable error message based on the error code."""
    if error_code == 404:
        return "Not Found"
    if error_code == 429:
        return "Too Many Requests"
    if error_code == 400:
        return "Bad Request"
    if error_code == 500:
        return "Internal Server Error"
    return f"Error {error_code}"


@dataclass
class DownloadChunk:
    """Data structure to hold download information."""

    channel: Channel
    date: date

    def sds_path(self, partial: bool = False) -> Path:
        """Return the SDS path for the channel on the specified date."""
        file_name = self.channel.sds_path(self.date)
        if partial:
            return file_name.parent / (file_name.name + ".partial")
        return file_name


class FDSNClientStats(Stats):
    """Statistics for the FDSN client."""

    _pos: int = 20

    n_requests: int = Field(
        default=0,
        description="Number of requests made to the FDSN service",
    )
    n_bytes_downloaded: int = Field(
        default=0,
        description="Total number of bytes downloaded from the FDSN service",
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
    _received_chunks: deque[tuple[float, int]] = PrivateAttr(
        default_factory=lambda: deque(maxlen=200)
    )

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

    def add_download_chunk(self, n_bytes: int, time: float) -> None:
        self._received_chunks.append((time, n_bytes))

    def chunk_add(self, channel: DownloadChunk) -> None:
        """Add a work item to the statistics."""
        self._station_work_count[channel.channel.nsl] += 1
        self.n_chunks_total += 1

    def chunk_done(self, channel: DownloadChunk) -> None:
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

    def get_download_speed(self, seconds: float = 10.0) -> ByteSize:
        """Calculate the average download speed across all downloads."""
        ref_time = asyncio.get_running_loop().time()
        measurements = [
            (time, n_bytes)
            for time, n_bytes in self._received_chunks
            if time >= (ref_time - seconds)
        ]
        if not measurements:
            return ByteSize(0)
        total_bytes = sum(n_bytes for _, n_bytes in measurements)
        first_time = measurements[0][0]
        return ByteSize(total_bytes / (ref_time - first_time))

    def _render(self, table: Table) -> None:
        """Render the statistics as a string."""
        if self._task_id is not None:
            self._progress.update(
                self._task_id,
                completed=self.n_completed,
                description=f"{human_readable_bytes(self.n_bytes_downloaded)}",
            )
        table.add_row(
            "Server",
            f"[bold]{self._client.url if self._client else 'N/A'}[/bold]"
            f" ↓ {self.get_download_speed().human_readable()}/s"
            f" ({self._client.n_workers if self._client else '?'} worker)",
        )
        table.add_row(
            "Stations",
            f"{self.n_stations_completed}/{self.n_stations} done",
        )
        table.add_row(
            "Progress",
            self._progress,
        )


class FDSNClient(BaseModel):
    url: HttpUrl = Field(
        default=HttpUrl("https://geofon.gfz.de"),
        description="Base URL of the FDSN web service",
    )
    timeout: float = Field(
        default=30.0,
        ge=1.0,
        description="Timeout for HTTP requests in seconds",
    )
    n_workers: int = Field(
        default=8,
        ge=1,
        le=64,
        description="Maximum number of concurrent connections",
    )
    chunk_size: ByteSizeStr = Field(
        default=ByteSize(4 * MB),
        ge=1 * MB,
        description="Length of data chunks to download in bytes",
    )
    rate_limit: int = Field(
        default=50,
        ge=1,
        description="Requests per second limit for the FDSN service, 0 for no limit",
    )

    available_stations: Stations = Field(
        default_factory=Stations,
        description="List of available stations from the FDSN service",
        exclude=True,
    )

    _stats: FDSNClientStats = PrivateAttr(default_factory=FDSNClientStats)
    _client: aiohttp.ClientSession | None = PrivateAttr(None)

    _work_queue: asyncio.Queue[DownloadChunk] = PrivateAttr(
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
                headers=HEADERS,
            ) as client,
            client.get(
                "/fdsnws/station/1/query",
                params=params,
            ) as response,
        ):
            logger.debug("Fetching available stations from %s", response.url)
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
                timeout=aiohttp.ClientTimeout(sock_read=self.timeout),
                headers=HEADERS,
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
        channel: Channel,
        date: date,
    ) -> AsyncGenerator[bytes, None]:
        """Fetch waveform data for a specific channel on a given date."""
        params = {
            "network": channel.nsl.network,
            "station": channel.nsl.station,
            "location": channel.nsl.location,
            "channel": channel.code,
            "starttime": date.isoformat(),
            "endtime": (date + timedelta(days=1)).isoformat(),
            "format": "mseed",
            "nodata": "404",
        }
        _clean_params(params)
        loop = asyncio.get_running_loop()
        start_time = datetime_now()

        async with client.get(
            "/fdsnws/dataselect/1/query",
            params=params,
            compress=True,
            raise_for_status=True,
        ) as response:
            self._stats.n_requests += 1
            rate_limit = response.headers.get("X-RateLimit-Limit", None)
            if rate_limit is not None:
                new_rate_limit = int(rate_limit)
                if self.rate_limit != new_rate_limit:
                    logger.warning(
                        "Rate limit changed from %d to %d requests per second",
                        self.rate_limit,
                        new_rate_limit,
                    )
                    self.rate_limit = new_rate_limit

            request_n_bytes = 0
            async for chunk in response.content.iter_chunked(self.chunk_size):
                chunk_size = len(chunk)
                request_n_bytes += chunk_size
                self._stats.n_bytes_downloaded += chunk_size
                self._stats.add_download_chunk(chunk_size, loop.time())

                yield chunk

        elapsed = datetime_now() - start_time
        speed = request_n_bytes / elapsed.total_seconds()

        logger.info(
            "Finished download %s.%s for %s (%s ↓%s/s)",
            channel.nsl.pretty,
            channel.code,
            date,
            human_readable_bytes(request_n_bytes),
            human_readable_bytes(speed),
        )

    async def add_work(self, download: DownloadChunk) -> None:
        """Add a download task to the work queue."""
        await self._work_queue.put(download)
        self._stats.chunk_add(download)

    async def download(self, writer: SDSWriter) -> None:
        """Download data from the FDSN service."""
        if self._work_queue.empty():
            raise ValueError("No work available in the queue")

        self._stats.start(self._work_queue.qsize())

        rate_limiter = asyncio.Condition()

        async def rate_limit_timer():
            while True:
                async with rate_limiter:
                    rate_limiter.notify()
                await asyncio.sleep(1.0 / self.rate_limit)

        async def worker(client: aiohttp.ClientSession) -> None:
            while not self._work_queue.empty():
                chunk = await self._work_queue.get()

                if writer.remote_log.has_error(
                    chunk.channel.nslc,
                    chunk.date,
                    self.url,
                ):
                    logger.warning(
                        "Skipping %s.%s for %s: already logged as error",
                        chunk.channel.nsl.pretty,
                        chunk.channel.code,
                        chunk.date,
                    )
                    self._work_queue.task_done()
                    continue

                async with rate_limiter:
                    await rate_limiter.wait()

                logger.info(
                    "Starting download %s.%s for %s",
                    chunk.channel.nsl.pretty,
                    chunk.channel.code,
                    chunk.date,
                )

                try:
                    async for data in self.download_waveform_data(
                        client=client,
                        channel=chunk.channel,
                        date=chunk.date,
                    ):
                        writer.add_data(chunk, data)
                except aiohttp.ClientResponseError as e:
                    logger.error(
                        "Failed to download %s for %s: %s error",
                        chunk.channel.nslc.pretty,
                        date,
                        get_error_str(e.code),
                    )
                    writer.remote_log.add_error(
                        chunk.channel.nslc,
                        chunk.date,
                        self.url,
                        e.code,
                    )
                    continue

                except TimeoutError:
                    logger.error(
                        "Failed to download %s for %s: Remote timeout (%.1f s)",
                        chunk.channel.nslc.pretty,
                        chunk.date,
                        self.timeout,
                    )
                    continue
                finally:
                    self._stats.chunk_done(chunk)
                    self._work_queue.task_done()

                writer.done(chunk)

        logger.info(
            "Starting download from %s with %d workers",
            self.url,
            self.n_workers,
        )

        rate_limit_task = asyncio.create_task(rate_limit_timer())
        try:
            client = aiohttp.ClientSession(
                base_url=str(self.url),
                timeout=aiohttp.ClientTimeout(sock_read=self.timeout),
                auto_decompress=True,
                headers=HEADERS,
            )
            async with asyncio.TaskGroup() as tg:
                for _ in range(self.n_workers):
                    tg.create_task(worker(client))
        finally:
            await client.close()
            rate_limit_task.cancel()

        logger.info("Download completed from %s", self.url)
