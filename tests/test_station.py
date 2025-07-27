from __future__ import annotations

from pathlib import Path

import pytest

from sdscopy.models.station import Channel, parse_stations

DATA_PATH = Path(__file__).parent / "data"

DATA_FILES = [DATA_PATH / "fdsnmeta-gfz.txt", DATA_PATH / "fdsnmeta-iris-HV.txt"]


@pytest.mark.parametrize("test_data", DATA_FILES)
def test_station_parse(test_data: Path) -> None:
    channels = []
    with open(test_data, "r") as f:
        for line in f:
            if line.startswith("#"):
                continue
            channel = Channel.from_line(line)
            channels.append(channel)


@pytest.mark.parametrize("test_data", DATA_FILES)
def test_station_load(test_data: Path) -> None:
    stations = parse_stations(test_data.read_text())
    assert len(stations) > 0

    for station in stations:
        assert station.n_channels > 0
