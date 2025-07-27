from __future__ import annotations

import logging
from datetime import date, datetime
from fnmatch import fnmatch
from itertools import groupby
from pathlib import Path

from pydantic import BaseModel, Field

from fdsn_download.utils import _NSL, AUX_CHANNELS, DATETIME_MAX, NSL

logger = logging.getLogger(__name__)

SDS_TEMPLATE: str = (
    "{year}/{network}/{station}/{channel}.D/"
    "{network}.{station}.{location}.{channel}.D.{year}.{julianday}"
)


class Channel(BaseModel):
    nsl: NSL
    code: str = Field(
        ...,
        max_length=3,
        min_length=3,
        description="Channel code",
    )

    lat: float = Field(
        ...,
        description="Latitude in degrees",
    )
    lon: float = Field(
        ...,
        description="Longitude in degrees",
    )
    elevation: float = Field(
        ...,
        description="Elevation in meters",
    )
    depth: float = Field(
        ...,
        description="Depth in meters",
    )
    azimuth: float = Field(
        ...,
        description="Azimuth in degrees",
    )
    dip: float = Field(
        ...,
        description="Dip in degrees",
    )
    sensor_description: str = Field(
        ...,
        description="Description of the sensor",
    )
    scale: float = Field(
        ...,
        description="Scale factor for the channel data",
    )
    scale_freq: float = Field(
        ...,
        description="Frequency of the scale factor in Hz",
    )
    scale_units: str = Field(
        ...,
        description="Units of the scale factor",
    )

    sampling_rate: float = Field(
        ...,
        description="Sampling rate in Hz",
    )

    start_time: datetime = Field(
        ...,
        description="Start time of the channel data",
    )
    end_time: datetime = Field(
        DATETIME_MAX,
        description="End time of the channel data",
    )

    def sds_path(self, date: date) -> Path:
        """Return the SDS path for the channel on the given date."""
        return Path(
            SDS_TEMPLATE.format(
                year=date.year,
                network=self.nsl.network,
                station=self.nsl.station,
                location=self.nsl.location,
                channel=self.code,
                julianday=date.timetuple().tm_yday,
            )
        )

    def matches(self, selector: str) -> bool:
        return fnmatch(self.code, selector)

    @classmethod
    def from_line(cls, line: str) -> Channel:
        # Network|Station|Location|Channel|Latitude|Longitude|Elevation|Depth|Azimuth|Dip|SensorDescription|Scale|ScaleFreq|ScaleUnits|SampleRate|StartTime|EndTime
        """Create a Channel instance from a line of text."""
        parts = line.strip().split("|")
        if len(parts) != 17:
            raise ValueError(f"Invalid line format: {line}, got {len(parts)} parts")
        parts = list(map(str.strip, parts))

        nsl = _NSL.parse(parts[0:3])
        return cls(
            nsl=nsl,
            code=parts[3],
            lat=float(parts[4]),
            lon=float(parts[5]),
            elevation=float(parts[6]),
            depth=float(parts[7]),
            azimuth=float(parts[8]),
            dip=float(parts[9]),
            sensor_description=parts[10],
            scale=float(parts[11]) if parts[11] else 0.0,
            scale_freq=float(parts[12]) if parts[12] else 0.0,
            scale_units=parts[13],
            sampling_rate=float(parts[14]),
            start_time=datetime.fromisoformat(parts[15]),
            end_time=datetime.fromisoformat(parts[16]) if parts[16] else DATETIME_MAX,
        )


class Station(BaseModel):
    nsl: NSL

    channels: list[Channel]

    @property
    def n_channels(self) -> int:
        """Return the number of channels in the station."""
        return len(self.channels)

    def get_channel_codes(self) -> set[str]:
        """Return a list of unique channel codes in the station."""
        return {c.code for c in self.channels}

    def get_channels(
        self,
        date: date,
        channel_selector: str = "*",
        min_sampling_rate: float = 0.0,
        max_sampling_rate: float = 0.0,
    ) -> list[Channel]:
        """Return channels that are valid on the given date."""
        channels = []
        for channel in self.channels:
            if not channel.start_time.date() <= date <= channel.end_time.date():
                continue
            if not channel.matches(channel_selector):
                continue
            if min_sampling_rate and channel.sampling_rate < min_sampling_rate:
                logger.warning(
                    "Channel %s has sampling rate %.2f, which is below the minimum %.2f",
                    channel.code,
                    channel.sampling_rate,
                    min_sampling_rate,
                )
                continue
            if max_sampling_rate and channel.sampling_rate > max_sampling_rate:
                logger.warning(
                    "Channel %s has sampling rate %.2f, which is above the maximum %.2f",
                    channel.code,
                    channel.sampling_rate,
                    max_sampling_rate,
                )
                continue
            channels.append(channel)
        return channels


class Stations(BaseModel):
    stations: list[Station] = Field(
        default_factory=list,
        description="List of stations",
    )

    def __iter__(self):
        """Iterate over the stations."""
        return iter(self.stations)

    @property
    def n_stations(self) -> int:
        """Return the number of stations."""
        return len(self.stations)

    def get_station(self, nsl: NSL) -> Station:
        """Return a station by its NSL."""
        for station in self.stations:
            if nsl.match(station.nsl):
                return station
        raise ValueError(f"Station {nsl} not found")


def parse_stations(data: str, ignore_aux: bool = True) -> Stations:
    """Parse metadata from a string and return a Station object."""
    lines = data.strip().splitlines()
    channels = []
    for line in lines:
        if line.startswith("#"):
            continue
        channel = Channel.from_line(line)
        if ignore_aux and channel.code in AUX_CHANNELS:
            continue
        channels.append(channel)

    if not channels:
        raise ValueError("No valid channel data found")

    stations = []

    for nsl, nsl_channels in groupby(channels, key=lambda c: c.nsl):
        stations.append(
            Station(
                nsl=nsl,
                channels=list(nsl_channels),
            )
        )

    for station in stations:
        station.channels.sort(key=lambda c: c.code)

    stations.sort(key=lambda s: s.nsl)
    return Stations(stations=stations)
