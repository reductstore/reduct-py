"""Batch helpers for ReductStore HTTP API"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, Callable

from aiohttp import ClientResponse

CHUNK_SIZE = 16_000
LABEL_PREFIX = "x-reduct-label-"
TIME_PREFIX = "x-reduct-time-"
ENTRIES_HEADER = "x-reduct-entries"
START_TS_HEADER = "x-reduct-start-ts"
LABELS_HEADER = "x-reduct-labels"
ERROR_PREFIX = "x-reduct-error-"


@dataclass
class BatchedRecord:
    entry: str
    timestamp: int
    size: int
    last: bool
    content_type: str
    labels: dict[str, str]
    read_all: Callable[[None], Awaitable[bytes]]
    read: Callable[[int], AsyncIterator[bytes]]


async def _read(buffer: list[bytes], n: int) -> AsyncIterator[bytes]:
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


async def _read_all(buffer: list[bytes]) -> bytes:
    return b"".join(buffer)


async def _read_response(resp: ClientResponse, content_length: int) -> list[bytes]:
    chunks = []
    count = 0
    while count < content_length:
        n = min(CHUNK_SIZE, content_length - count)
        chunk = await resp.content.read(n)
        chunks.append(chunk)
        count += len(chunk)

    return chunks


from .v1 import parse_batched_records_v1  # noqa: E402
from .v1 import make_headers_v1  # noqa: E402
from .v2 import parse_batched_records_v2, make_headers_v2  # noqa: E402

__all__ = [
    "BatchedRecord",
    "LABEL_PREFIX",
    "TIME_PREFIX",
    "ENTRIES_HEADER",
    "START_TS_HEADER",
    "LABELS_HEADER",
    "ERROR_PREFIX",
    "parse_batched_records_v1",
    "parse_batched_records_v2",
    "make_headers_v1",
    "make_headers_v2",
]
