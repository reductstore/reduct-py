"""Record module"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Callable,
    AsyncIterator,
    Awaitable,
)

from aiohttp import ClientResponse

from reduct.batch import (
    BatchedRecord,
    LABEL_PREFIX,
    parse_batched_records_v1,
    parse_batched_records_v2,
)
from reduct.time import (
    unix_timestamp_to_datetime,
    unix_timestamp_from_any,
    TimestampLike,
)


@dataclass
class Record:
    """Record in a query"""

    entry: str
    """entry name of record"""
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


@dataclass
class BatchItem:
    """Single item in a batch request"""

    entry: str | None
    timestamp: int
    data: bytes
    content_type: str
    labels: dict[str, str]

    @property
    def size(self) -> int:
        """Size of the payload"""
        return len(self.data)


class Batch:
    """Batch of records to write them in one request"""

    def __init__(self):
        self._items: list[BatchItem] = []
        self._total_size = 0
        self._last_access = 0

    def add(
        self,
        timestamp: TimestampLike,
        data: bytes | None = b"",
        content_type: str | None = None,
        labels: dict[str, str] | None = None,
        entry: str | None = None,
    ):
        """Add record to batch
        Args:
            timestamp: timestamp of record. int (UNIX timestamp in microseconds),
                datetime, float (UNIX timestamp in seconds), str (ISO 8601 string)
            data: data to store
            content_type: content type of data (default: application/octet-stream)
            labels: labels of record (default: {})
            entry: explicit entry for the record. If None, the bucket entry passed to
                the API will be used.
        """
        if content_type is None:
            content_type = ""

        if labels is None:
            labels = {}

        payload = data if data is not None else b""

        record = BatchItem(
            entry=entry,
            timestamp=unix_timestamp_from_any(timestamp),
            data=payload,
            content_type=content_type,
            labels=labels,
        )

        self._total_size += record.size
        self._last_access = time.time()
        self._items.append(record)

    def items(self, default_entry: str | None = None) -> list[BatchItem]:
        """Get records sorted by entry and timestamp.

        Args:
            default_entry: entry to use when a record doesn't have an explicit entry.
        """
        return self.sorted_items(default_entry)

    def sorted_items(self, default_entry: str | None = None) -> list[BatchItem]:
        """Return records sorted by entry and timestamp.

        Records without an explicit entry fall back to ``default_entry``.
        """
        resolved = []
        for item in self._items:
            entry = item.entry or default_entry
            if entry is None:
                raise ValueError("Entry is not specified for a batch item")

            resolved.append(
                BatchItem(
                    entry=entry,
                    timestamp=item.timestamp,
                    data=item.data,
                    content_type=item.content_type,
                    labels=item.labels,
                )
            )

        return sorted(resolved, key=lambda item: (item.entry, item.timestamp))

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
        self._items.clear()
        self._total_size = 0
        self._last_access = 0

    def __len__(self):
        return len(self._items)


def parse_record(resp: ClientResponse, last=True, entry: str = "") -> Record:
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
        entry=entry,
        timestamp=timestamp,
        size=size,
        last=last,
        read_all=resp.read,
        read=resp.content.iter_chunked,
        labels=labels,
        content_type=content_type,
    )


def _batched_to_record(batched: BatchedRecord) -> Record:
    return Record(
        entry=batched.entry,
        timestamp=batched.timestamp,
        size=batched.size,
        last=batched.last,
        content_type=batched.content_type,
        labels=batched.labels,
        read_all=batched.read_all,
        read=batched.read,
    )


async def parse_batched_records(resp: ClientResponse) -> AsyncIterator[Record]:
    """Parse batched records from response"""

    parsed_v2 = await parse_batched_records_v2(resp)
    if parsed_v2 is not None:
        async for record in parsed_v2:
            yield _batched_to_record(record)
        return

    async for record in parse_batched_records_v1(resp):
        yield _batched_to_record(record)
