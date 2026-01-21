"""Record module"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import (
    Callable,
    AsyncIterator,
    Awaitable,
)

from aiohttp import ClientResponse

from reduct.time import unix_timestamp_to_datetime


@dataclass
class Record:  # pylint: disable=too-many-instance-attributes
    """Record in a query"""

    timestamp: int
    """UNIX timestamp in microseconds"""
    entry: str | None
    """entry name"""
    size: int
    """size of data"""
    last: bool
    """last record in the query. Deprecated: doesn't work for some cases"""
    content_type: str
    """content type of data"""
    read_all: Callable[[None], Awaitable[bytes]]
    """read all data"""
    read: Callable[[int], AsyncIterator[bytes]]
    """read data in chunks where each chunk has size less than or equal to n"""

    labels: dict[str, str]
    """labels of record"""

    def get_datetime(self) -> datetime:
        """Get timestamp of record as datetime
        Returns:
            datetime: timestamp as datetime
        """
        return unix_timestamp_to_datetime(self.timestamp)


LABEL_PREFIX = "x-reduct-label-"
TIME_PREFIX = "x-reduct-time-"
ERROR_PREFIX = "x-reduct-error-"


def parse_record(resp: ClientResponse, entry_name: str, last=True) -> Record:
    """Parse record from response"""
    timestamp = int(resp.headers["x-reduct-time"])
    size = int(resp.headers["content-length"])
    content_type = resp.headers.get("content-type", "application/octet-stream")
    labels = dict(
        (name[len(LABEL_PREFIX) :], value)
        for name, value in resp.headers.items()
        if name.startswith(LABEL_PREFIX)
    )

    return Record(
        timestamp=timestamp,
        entry=entry_name,
        size=size,
        last=last,
        read_all=resp.read,
        read=resp.content.iter_chunked,
        labels=labels,
        content_type=content_type,
    )
