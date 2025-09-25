from __future__ import annotations

import logging
from datetime import date, datetime
from hashlib import sha1
from pathlib import Path
from typing import NamedTuple

from pydantic import HttpUrl

from fdsn_download.utils import NSLC, datetime_now

logger = logging.getLogger(__name__)

LOG_ERROR_CODES = {404}


def _hash_error(nslc: NSLC, date: date, host: str) -> bytes:
    return sha1(f"{nslc.pretty}{date}{host}".encode("utf-8")).digest()


class RemoteError(NamedTuple):
    nslc: NSLC
    date: date
    host: str
    error_code: int
    time: datetime

    def as_csv(self) -> str:
        """Return a CSV representation of the error."""
        return (
            f"{self.nslc.pretty},{self.date},"
            f"{self.host},{self.error_code},{self.time.isoformat()}"
        )

    def hash(self) -> bytes:
        """Return a hash of the error for quick comparison."""
        return _hash_error(self.nslc, self.date, self.host)

    @classmethod
    def from_csv(cls, csv_line: str) -> RemoteError:
        """Create a RemoteError from a CSV line."""
        nslc_str, date_, host, error_code, time = csv_line.split(",")
        return cls(
            nslc=NSLC.from_string(nslc_str),
            date=date.fromisoformat(date_),
            host=host,
            error_code=int(error_code),
            time=datetime.fromisoformat(time),
        )


class RemoteLog:
    """Log of remote files with errors."""

    errors: list[RemoteError]
    _error_hash: dict[bytes, int]

    def __init__(self, log_file: Path | None = None):
        self.errors: list[RemoteError] = []
        self._error_hash: dict[bytes, int] = {}
        if log_file and log_file.exists():
            self.set_logfile(log_file)

    @property
    def n_errors(self) -> int:
        """Return the number of errors in the log."""
        return len(self.errors)

    def set_logfile(self, file: Path) -> None:
        """Load the log from a file."""
        logger.debug("Setting remote log file to %s", file)
        if file.exists():
            n_loaded = 0
            with file.open("r") as f:
                for line in f:
                    error = RemoteError.from_csv(line.strip())
                    self.errors.append(error)
                    self._error_hash[error.hash()] = error.error_code
                    n_loaded += 1
            logger.info("Loaded %d remote errors from %s", n_loaded, file)
        if not file.parent.exists():
            file.parent.mkdir(parents=True, exist_ok=True)
        self.file = file

    def add_error(
        self,
        nslc: NSLC,
        date: date,
        remote: HttpUrl,
        error_code: int,
    ) -> None:
        """Add an error to the log."""
        if error_code not in LOG_ERROR_CODES:
            return
        if not remote.host:
            raise ValueError("Remote URL must have a host")
        error = RemoteError(nslc, date, remote.host, error_code, datetime_now())
        self.errors.append(error)
        self._error_hash[error.hash()] = error_code
        if self.file:
            with self.file.open("a") as f:
                f.write(error.as_csv() + "\n")

    def get_error(self, nslc: NSLC, date: date, remote: HttpUrl) -> int | None:
        """Get the remote error for the given NSLC and remote URL."""
        if not remote.host:
            raise ValueError("Remote URL must have a host")
        return self._error_hash.get(_hash_error(nslc, date, remote.host), None)
