"""Shared helpers for batch protocols."""

from functools import partial
import asyncio
import time
from typing import AsyncIterator, Awaitable, Callable

from reduct.record import Record
from reduct.time import TimestampLike, unix_timestamp_from_any


class BaseBatch:
    """Batch of records to write them in one request"""

    def __init__(self):
        self._records: dict[tuple[str, int], Record] = {}
        self._total_size = 0
        self._last_access = 0

    def _add(
        self,
        entry: str,
        timestamp: TimestampLike,
        data: bytes = b"",
        **kwargs,
    ):

        content_type = kwargs.pop("content_type", None)
        if content_type is None:
            content_type = "application/octet-stream"

        labels = kwargs.pop("labels", None)
        if labels is None:
            labels = {}

        rec_offset = 0

        async def read_chunk(n: int) -> AsyncIterator[bytes]:
            nonlocal rec_offset
            while rec_offset < len(data):
                chunk = data[rec_offset : rec_offset + n]
                rec_offset += len(chunk)
                yield chunk

        async def read_all_data() -> bytes:
            return data

        ts = unix_timestamp_from_any(timestamp)
        record = Record(
            entry=entry,
            timestamp=ts,
            size=len(data),
            content_type=content_type,
            labels=labels,
            read_all=read_all_data,
            read=read_chunk,
            last=False,
        )

        self._total_size += record.size
        self._last_access = time.time()
        self._records[(entry, ts)] = record

    def items(self) -> list[tuple[tuple[str, int], Record]]:
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


async def read_all(buffer: list[bytes]) -> bytes:
    """Join buffered chunks into a single bytes payload."""
    return b"".join(buffer)


CHUNK_SIZE = 16_000


async def read_response(resp, content_length) -> list[bytes]:
    """Read a response body into fixed-size chunks."""
    chunks = []
    count = 0
    while count < content_length:
        n = min(CHUNK_SIZE, content_length - count)
        chunk = await resp.content.read(n)
        chunks.append(chunk)
        count += len(chunk)

    return chunks


async def read(buffer: list[bytes], n: int) -> AsyncIterator[bytes]:
    """Yield buffered chunks in sizes up to n bytes."""
    while len(buffer) > 0:
        part = buffer.pop(0)
        if len(part) == 0:
            continue

        count = 0
        size = len(part)
        m = min(n, size)

        while count < size:
            chunk = part[count : count + m]
            count += len(chunk)
            m = min(m, size - count)
            yield chunk
            await asyncio.sleep(0)


async def prepare_batched_record_read(
    resp,
    content_length: int,
    record_index: int,
    records_total: int,
    head: bool,
) -> tuple[
    Callable[[int], AsyncIterator[bytes]],
    Callable[[], Awaitable[bytes]],
    bool,
]:
    """Build read helpers for a batched record response."""
    last = False
    if record_index == records_total:
        read_func = resp.content.iter_chunked
        read_all_func = resp.read
        if resp.headers.get("x-reduct-last", "false") == "true":
            last = True
    else:
        if head:
            buffer = []
        else:
            buffer = await read_response(resp, content_length)
        read_func = partial(read, buffer)
        read_all_func = partial(read_all, buffer)
    return read_func, read_all_func, last
