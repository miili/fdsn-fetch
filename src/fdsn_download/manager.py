from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, PrivateAttr, field_validator
from rich.progress import track

from fdsn_download.client import DownloadDayfile, FDSNClient
from fdsn_download.stats import Stats
from fdsn_download.utils import _NSL, NSL, Date, date_today, datetime_now
from fdsn_download.writer import SDSWriter

if TYPE_CHECKING:
    from rich.table import Table

logger = logging.getLogger(__name__)


class FDSNDownloadManagerStats(Stats):
    _pos: int = 0

    start_time: datetime | None = Field(
        default=None,
        title="Start Time",
        description="Time when the download started",
    )

    def _render(self, table: Table) -> None:
        """Render the statistics as a string."""
        if self.start_time:
            elapsed_time = str(datetime_now() - self.start_time).split(".")[0]
        else:
            elapsed_time = "waiting..."

        table.add_row("Time elapsed", elapsed_time)


class FDSNDownloadManager(BaseModel):
    writer: SDSWriter = Field(
        default_factory=SDSWriter,
        description="Writer for storing downloaded SDS data",
    )
    clients: list[FDSNClient] = Field(
        default_factory=lambda: [FDSNClient()],
        description="List of FDSN clients for downloading data",
    )
    metadata_path: Path = Field(
        default=Path("./metadata/"),
        description="Path to store downloaded metadata",
    )
    time_range: tuple[Date, Date] = Field(
        default_factory=lambda: (date_today() - timedelta(days=7), date_today()),
        description="Time range for downloading data",
    )
    station_selection: list[NSL] = Field(
        default=[_NSL("2D", "", "")],
        description="List of NSL selections for stations to download",
    )
    channel_priority: list[str] = Field(
        default=["HH[ZNE12]", "EH[ZNE12]", "HN[ZNE12]"],
        description="List of channel codes to download",
    )
    station_blacklist: set[NSL] = Field(
        default_factory=set,
        description="List of NSL selections for stations to exclude from download",
    )
    min_channels_per_station: int = Field(
        default=3,
        description="Minimum number of channels required per day.",
    )
    min_sampling_rate: float = Field(
        default=100.0,
        description="Minimum sampling rate for channels to be downloaded",
    )
    max_sampling_rate: float = Field(
        default=200.0,
        description="Maximum sampling rate for channels to be downloaded",
    )

    _file: Path | None = PrivateAttr(default=None)
    _stats: FDSNDownloadManagerStats = PrivateAttr(
        default_factory=FDSNDownloadManagerStats
    )

    @classmethod
    def load(cls, file: Path) -> FDSNDownloadManager:
        """Load the configuration from a JSON file."""
        if not file.exists():
            raise FileNotFoundError(f"Configuration file {file} does not exist")
        model = cls.model_validate_json(file.read_text(), strict=True)
        model._file = file
        return model

    @field_validator("time_range")
    @classmethod
    def validate_time_range(cls, value: tuple[date, date]) -> tuple[date, date]:
        """Ensure the time range is valid."""
        start, end = value
        if start > end:
            raise ValueError("Start date must be before end date")
        return value

    async def prepare(self):
        """Prepare the download manager by initializing the writer and clients."""
        for client in self.clients:
            await client.prepare(
                self.station_selection,
                self.time_range[0],
                self.time_range[1],
            )
        await self.writer.prepare()

        self._stats.start_time = datetime_now()

    def get_available_stations(self) -> list[NSL]:
        """Get a list of available stations based on the selection and blacklist."""
        available_stations = []
        for client in self.clients:
            for station in client.available_stations:
                if not any(nsl.match(station.nsl) for nsl in self.station_selection):
                    continue
                if any(nsl.match(station.nsl) for nsl in self.station_blacklist):
                    continue
                available_stations.append(station.nsl)
        return available_stations

    def get_work(self, client: FDSNClient) -> list[DownloadDayfile]:
        chunks: list[DownloadDayfile] = []

        for station in client.available_stations:
            if not any(nsl.match(station.nsl) for nsl in self.station_selection):
                continue
            if any(nsl.match(station.nsl) for nsl in self.station_blacklist):
                continue

            date = self.time_range[0]
            while date + timedelta(days=1) <= self.time_range[1]:
                for channel_selector in self.channel_priority:
                    station_chunks = []
                    for channel in station.get_channels(
                        date,
                        channel_selector,
                        self.min_sampling_rate,
                        self.max_sampling_rate,
                    ):
                        station_chunks.append(
                            DownloadDayfile(channel=channel, date=date)
                        )
                    if len(station_chunks) < self.min_channels_per_station:
                        logger.debug(
                            "Station %s has %d channels for %s, need at least %d",
                            station.nsl.pretty,
                            len(station_chunks),
                            date,
                            self.min_channels_per_station,
                        )
                        continue
                    chunks.extend(station_chunks)
                    break

                date += timedelta(days=1)

        logger.info("Discovered %d remote dayfiles", len(chunks))
        chunks_download = []
        for channel in track(chunks, description="Checking SDS archive..."):
            if not self.writer.has_chunk(channel):
                chunks_download.append(channel)

        i_downloaded = len(chunks) - len(chunks_download)
        if i_downloaded > 0:
            logger.info(
                "Found %d dayfiles already downloaded in the archive", i_downloaded
            )

        logger.info("Found %d dayfiles to download", len(chunks_download))
        return chunks_download

    async def _download_from_client(self, client: FDSNClient, writer: SDSWriter):
        """Download data for the specified client."""
        work = self.get_work(client)
        if not work:
            logger.info("No work found for client %s", client.url)
            return

        for channel in work:
            await client.add_work(channel)

        await client.download(writer)

    async def download(self):
        """Download data using all configured clients."""
        await self.prepare()

        async with asyncio.TaskGroup() as tg:
            for client in self.clients:
                tg.create_task(self._download_from_client(client, self.writer))
        logger.info("All downloads completed successfully.")
        await self.download_metadata()

    async def download_metadata(self):
        """Download metadata for the selected stations."""
        for client in self.clients:
            available_stations = []
            for station in client.available_stations:
                if not any(nsl.match(station.nsl) for nsl in self.station_selection):
                    continue
                if any(nsl.match(station.nsl) for nsl in self.station_blacklist):
                    continue
                available_stations.append(station.nsl)

            self.metadata_path.mkdir(parents=True, exist_ok=True)
            for network, stations in groupby(
                available_stations, key=lambda x: x.network
            ):
                stations = list(stations)
                logger.info(
                    "Downloading metadata for network %s (%d stations)",
                    network,
                    len(stations),
                )
                data = await client.download_metadata(
                    stations,
                    self.time_range[0],
                    self.time_range[1],
                )
                metadata_file = self.metadata_path / f"{network}.xml"
                metadata_file.write_text(data)

        logger.info("Metadata download completed successfully.")
