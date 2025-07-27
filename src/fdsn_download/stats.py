from __future__ import annotations

import asyncio
import random
import string
from typing import Any, Iterator, NoReturn
from weakref import WeakValueDictionary

from pydantic import BaseModel
from pydantic.fields import ComputedFieldInfo, FieldInfo
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

STATS_INSTANCES: WeakValueDictionary[str, Stats] = WeakValueDictionary()


async def live_view() -> NoReturn:
    def generate_grid() -> Table:
        """Make a new table."""
        table = Table(show_header=False, box=None, expand=True)
        stats_instances = sorted(STATS_INSTANCES.values(), key=lambda x: x._pos)
        for stats in stats_instances:
            table.add_section()
            stats._render(table)

        grid = Table.grid(expand=True)
        grid.add_row(Panel(table, title="FDSN Download"))
        return grid

    with Live(
        refresh_per_second=4,
        # screen=True,
    ) as live:
        while True:
            live.update(generate_grid())
            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                break


class Stats(BaseModel):
    _pos: int = 10

    def _render(self, table: Table) -> None:
        """Render the statistics as a string."""
        for name, field in self.iter_fields():
            title = field.title
            table.add_row(
                title,
                str(getattr(self, name)),
                style="dim",
            )

    @classmethod
    def get_subclasses(cls) -> set[type[Stats]]:
        """Return a set of all subclasses of Stats."""
        return set(cls.__subclasses__())

    def iter_fields(self) -> Iterator[tuple[str, FieldInfo | ComputedFieldInfo]]:
        """Iterate over the fields of the Stats model."""
        yield from self.model_fields.items()
        yield from self.model_computed_fields.items()

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization hook for Stats."""
        uid = "".join(random.choices(string.ascii_uppercase + string.digits, k=16))
        STATS_INSTANCES[uid] = self
