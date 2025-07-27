from __future__ import annotations

import pytest

from fdsn_download.manager import FDSNDownloadManager


@pytest.mark.asyncio
async def test_manager():
    manager = FDSNDownloadManager()
    await manager.prepare()
    for client in manager.clients:
        manager.get_work(client)

    await manager.download()
