"""Record module"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import (
    Callable,
    AsyncIterator,
    Awaitable,
)

from aiohttp import ClientResponse

from reduct.time import (
    unix_timestamp_to_datetime,
    unix_timestamp_from_any,
    TimestampLike,
)


@dataclass
class Record:
    """Record in a query"""

    timestamp: int
    """UNIX timestamp in microseconds"""
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


class Batch:
    """Batch of records to write them in one request"""

    def __init__(self):
        self._records: dict[int, Record] = {}
        self._total_size = 0
        self._last_access = 0

    def add(
        self,
        timestamp: TimestampLike,
        data: bytes = b"",
        content_type: str | None = None,
        labels: dict[str, str] | None = None,
    ):
        """Add record to batch
        Args:
            timestamp: timestamp of record. int (UNIX timestamp in microseconds),
                datetime, float (UNIX timestamp in seconds), str (ISO 8601 string)
            data: data to store
            content_type: content type of data (default: application/octet-stream)
            labels: labels of record (default: {})
        """
        if content_type is None:
            content_type = ""

        if labels is None:
            labels = {}

        rec_offset = 0

        async def read(n: int) -> AsyncIterator[bytes]:
            nonlocal rec_offset
            while rec_offset < len(data):
                chunk = data[rec_offset : rec_offset + n]
                rec_offset += len(chunk)
                yield chunk

        async def read_all() -> bytes:
            return data

        record = Record(
            timestamp=unix_timestamp_from_any(timestamp),
            size=len(data),
            content_type=content_type,
            labels=labels,
            read_all=read_all,
            read=read,
            last=False,
        )

        self._total_size += record.size
        self._last_access = time.time()
        self._records[record.timestamp] = record

    def items(self) -> list[tuple[int, Record]]:
        """Get records as dict items"""
        return sorted(self._records.items())

    @property
    def size(self) -> int:
        """Get size of data in batch"""
        return self._total_size

    @property
    def last_access(self) -> float:
        """Get last access time of batch. Can be used for sending by timeout"""
        return self._last_access

    def clear(self):
        """Clear batch"""
        self._records.clear()
        self._total_size = 0
        self._last_access = 0

    def __len__(self):
        return len(self._records)


LABEL_PREFIX = "x-reduct-label-"
TIME_PREFIX = "x-reduct-time-"
ERROR_PREFIX = "x-reduct-error-"


def parse_record(resp: ClientResponse, last=True) -> Record:
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
        size=size,
        last=last,
        read_all=resp.read,
        read=resp.content.iter_chunked,
        labels=labels,
        content_type=content_type,
    )
