import pytest
from pydantic import HttpUrl

from sdscopy.client import FDSNClient
from sdscopy.utils import _NSL, date_today


@pytest.mark.asyncio
async def test_client():
    client = FDSNClient(url=HttpUrl("https://geofon.gfz.de"))
    await client.prepare(selection=[_NSL(network="2D", station="", location="")])

    assert client.n_station() > 0

    async for data in client.download_waveform_data(
        channel=client.available_stations[0].channels[0],
        date=date_today(),
    ):
        assert isinstance(data, bytes)
        assert len(data) > 0
