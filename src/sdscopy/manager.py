from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from itertools import groupby
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

from sdscopy.client import DownloadChannel, FDSNClient
from sdscopy.utils import _NSL, NSL, date_today
from sdscopy.writer import SDSWriter

logger = logging.getLogger(__name__)


class FDSNDownloadManager(BaseModel):
    writer: SDSWriter = Field(
        default_factory=SDSWriter,
        description="Writer for storing downloaded SDS data",
    )
    metadata: Path = Field(
        default=Path("metadata/"),
        description="Path to store downloaded metadata",
    )
    time_range: tuple[date, date] = Field(
        default_factory=lambda: (date_today() - +timedelta(days=7), date_today()),
        description="Time range for downloading data",
    )
    channel_selection: list[str] = Field(
        default=["HH[ZNE12]"],
        description="List of channel codes to download",
    )
    min_channels_per_station: int = Field(
        default=3,
        description="Minimum number of channels per station to download",
    )
    min_sampling_rate: float = Field(
        default=100.0,
        description="Minimum sampling rate for channels to be downloaded",
    )
    max_sampling_rate: float = Field(
        default=200.0,
        description="Maximum sampling rate for channels to be downloaded",
    )
    clients: list[FDSNClient] = Field(
        default_factory=lambda: [FDSNClient()],
        description="List of FDSN clients for downloading data",
    )
    station_selection: list[NSL] = Field(
        default=[_NSL("2D", "", "")],
        description="List of NSL selections for stations to download",
    )
    station_blacklist: list[NSL] = Field(
        default_factory=list,
        description="List of NSL selections for stations to exclude from download",
    )

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

        self.writer.clean_partial_files()

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

    def get_work(self, client: FDSNClient) -> list[DownloadChannel]:
        chunks: list[DownloadChannel] = []

        for station in client.available_stations:
            if not any(nsl.match(station.nsl) for nsl in self.station_selection):
                continue
            if any(nsl.match(station.nsl) for nsl in self.station_blacklist):
                continue

            date = self.time_range[0]
            while date <= self.time_range[1]:
                station_chunks = []
                for channel_selector in self.channel_selection:
                    for channel in station.get_channels(
                        date,
                        channel_selector,
                        self.min_sampling_rate,
                        self.max_sampling_rate,
                    ):
                        station_chunks.append(
                            DownloadChannel(channel=channel, date=date)
                        )
                    if len(station_chunks) >= self.min_channels_per_station:
                        chunks.extend(station_chunks)
                        break

                date += timedelta(days=1)

        for channel in chunks.copy():
            if self.writer.has_channel(channel):
                chunks.remove(channel)

        return chunks

    async def _download_client(self, client: FDSNClient):
        """Download data for the specified client."""
        work = self.get_work(client)
        if not work:
            logger.info("No work found for client %s", client.url)
            return

        for channel in work:
            await client.add_work(channel)

        await client.download(self.writer)

    async def download(self):
        """Download data using all configured clients."""
        await self.prepare()

        async with asyncio.TaskGroup() as tg:
            for client in self.clients:
                tg.create_task(self._download_client(client))
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

            self.metadata.mkdir(parents=True, exist_ok=True)
            for network, stations in groupby(
                available_stations, key=lambda x: x.network
            ):
                stations = list(stations)
                logger.info(
                    "Downloading metadata for network %s, %d stations",
                    network,
                    len(stations),
                )
                data = await client.download_metadata(
                    stations,
                    self.time_range[0],
                    self.time_range[1],
                )
                metadata_file = self.metadata / f"{network}.xml"
                metadata_file.write_text(data)

        logger.info("Metadata download completed successfully.")
