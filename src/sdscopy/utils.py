from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Annotated, NamedTuple

from pydantic import AfterValidator, BeforeValidator, ByteSize

DATETIME_MAX = datetime.max.replace(tzinfo=timezone.utc)
DATETIME_MIN = datetime.min.replace(tzinfo=timezone.utc)

AUX_CHANNELS = {
    "HDF",
    "BDF",
    "LCE",
    "LCL",
    "LCQ",
    "SBT",
    "SCA",
    "SCE",
    "SCK",
    "SCS",
    "SCT",
    "SDG",
    "SDL",
    "SDT",
    "SIO",
    "SMD",
    "SNI",
    "SPK",
    "SPO",
    "SRD",
    "SSL",
    "SSQ",
    "STH",
    "SWR",
    "VAP",
    "VCO",
    "VE1",
    "VE2",
    "VEA",
    "VEC",
    "VEP",
    "VFP",
    "VKI",
    "VM1",
    "VM2",
    "VM3",
    "LDO",
}


class _NSL(NamedTuple):
    network: str
    station: str
    location: str

    @property
    def pretty(self) -> str:
        return ".".join(self)

    def match(self, other: NSL) -> bool:
        """Check if the current NSL object matches another NSL object.

        Args:
            other (NSL): The NSL object to compare with.

        Returns:
            bool: True if the objects match, False otherwise.

        """
        if self.location:
            return self == other
        if self.station:
            return self.network == other.network and self.station == other.station
        return self.network == other.network

    @classmethod
    def parse(cls, nsl: str | NSL | list[str] | tuple[str, str, str]) -> NSL:
        """Parse the given NSL string and return an NSL object.

        Args:
            nsl (str): The NSL string to parse.

        Returns:
            NSL: The parsed NSL object.

        Raises:
            ValueError: If the NSL string is empty or invalid.

        """
        if not nsl:
            raise ValueError(f"invalid empty NSL: {nsl}")
        if type(nsl) is _NSL:
            return nsl
        if isinstance(nsl, (list, tuple)):
            return cls(*nsl)
        if not isinstance(nsl, str):
            raise ValueError(f"invalid NSL {nsl}")

        parts = nsl.split(".")
        n_parts = len(parts)
        if n_parts >= 3:
            return cls(*parts[:3])
        if n_parts == 2:
            return cls(parts[0], parts[1], "")
        if n_parts == 1:
            return cls(parts[0], "", "")
        raise ValueError(
            f"invalid NSL `{nsl}`, expecting `<net>.<sta>.<loc>`, "
            "e.g. `6A.STA130.00`, `6A.`, `6A.STA130` or `.STA130`"
        )

    def _check(self) -> NSL:
        """Check if the current NSL object matches another NSL object.

        Args:
            nsl (NSL): The NSL object to compare with.

        Returns:
            bool: True if the objects match, False otherwise.

        """
        if len(self.network) > 2:
            raise ValueError(
                f"invalid network {self.network} for {self.pretty},"
                " expected 0-2 characters for network code"
            )
        if len(self.station) > 5:
            raise ValueError(
                f"invalid station {self.station} for {self.pretty},"
                " expected 0-5 characters for station code"
            )
        if len(self.location) > 2:
            raise ValueError(
                f"invalid location {self.location} for {self.pretty},"
                " expected 0-2 characters for location code"
            )
        return self


NSL = Annotated[_NSL, BeforeValidator(_NSL.parse), AfterValidator(_NSL._check)]


def datetime_now() -> datetime:
    """Return the current UTC datetime."""
    return datetime.now(timezone.utc)


def date_today() -> date:
    """Return the current UTC date."""
    return datetime_now().date()


def human_readable_bytes(size: int | float, decimal: bool = False) -> str:
    """Convert a size in bytes to a human-readable string representation.

    Args:
        size (int | float): The size in bytes.
        decimal: If True, use decimal units (e.g. 1000 bytes per KB).
            If False, use binary units (e.g. 1024 bytes per KiB).

    Returns:
        str: The human-readable string representation of the size.

    """
    return ByteSize(size).human_readable(decimal=decimal)
