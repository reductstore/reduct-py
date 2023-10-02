"""Record representation and its parsing"""
import asyncio
from dataclasses import dataclass
from functools import partial
from typing import Dict, Callable, AsyncIterator, Awaitable, Optional, List, Tuple

from aiohttp import ClientResponse


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
    """read data in chunks"""

    labels: Dict[str, str]
    """labels of record"""


class Batch:
    """Batch of records to write them in one request"""

    def __init__(self):
        self._records: Dict[int, Record] = {}

    def add(
        self,
        timestamp: int,
        data: bytes,
        content_type: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
    ):
        """Add record to batch
        Args:
            timestamp: UNIX timestamp in microseconds
            data: data to store
            content_type: content type of data (default: application/octet-stream)
            labels: labels of record (default: {})
        """
        if content_type is None:
            content_type = "application/octet-stream"
        if labels is None:
            labels = {}

        def read(n: int) -> AsyncIterator[bytes]:
            raise NotImplementedError()

        async def read_all():
            return data

        record = Record(
            timestamp=timestamp,
            size=len(data),
            content_type=content_type,
            labels=labels,
            read_all=read_all,
            read=read,
            last=False,
        )

        self._records[timestamp] = record

    def items(self) -> List[Tuple[int, Record]]:
        """Get records as dict items"""
        return sorted(self._records.items())


LABEL_PREFIX = "x-reduct-label-"
TIME_PREFIX = "x-reduct-time-"
ERROR_PREFIX = "x-reduct-error-"
CHUNK_SIZE = 512_000


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


def _parse_header_as_csv_row(row: str) -> (int, str, Dict[str, str]):
    items = []
    escaped = ""
    for item in row.split(","):
        if item.startswith('"') and not escaped:
            escaped = item[1:]
        if escaped:
            if item.endswith('"'):
                escaped = escaped[:-1]
                items.append(escaped)
                escaped = ""
            else:
                escaped += item
        else:
            items.append(item)

    content_length = int(items[0])
    content_type = items[1]

    labels = {}
    for label in items[2:]:
        if "=" in label:
            name, value = label.split("=", 1)
            labels[name] = value

    return content_length, content_type, labels


async def _read(buffer: bytes, n: int):
    count = 0
    size = len(buffer)
    n = min(n, size)

    while True:
        chunk = buffer[count : count + n]
        count += len(chunk)
        n = min(n, size - count)
        yield chunk

        await asyncio.sleep(0)

        if count == size:
            break


async def _read_all(buffer):
    data = b""
    async for chunk in _read(buffer, CHUNK_SIZE):
        data += chunk
    return data


async def parse_batched_records(resp: ClientResponse) -> AsyncIterator[Record]:
    """Parse batched records from response"""

    records_total = sum(1 for header in resp.headers if header.startswith(TIME_PREFIX))
    records_count = 0
    head = resp.method == "HEAD"

    for name, value in resp.headers.items():
        if name.startswith(TIME_PREFIX):
            timestamp = int(name[14:])
            content_length, content_type, labels = _parse_header_as_csv_row(value)

            last = False
            records_count += 1

            if records_count == records_total:
                # last record in batched records read in client code
                read_func = resp.content.iter_chunked
                read_all_func = resp.read
                if resp.headers.get("x-reduct-last", "false") == "true":
                    # last record in query
                    last = True
            else:
                # batched records must be read in order, so it is safe to read them here
                # instead of reading them in the use code with an async interator.
                # The batched records are small if they are not the last.
                # The last batched record is read in the async generator in chunks.
                if head:
                    buffer = b""
                else:
                    buffer = await _read_response(resp, content_length)
                read_func = partial(_read, buffer)
                read_all_func = partial(_read_all, buffer)

            record = Record(
                timestamp=timestamp,
                size=content_length,
                last=last,
                content_type=content_type,
                labels=labels,
                read_all=read_all_func,
                read=read_func,
            )

            yield record


async def _read_response(resp, content_length):
    buffer = b""
    count = 0
    while True:
        n = min(CHUNK_SIZE, content_length - count)
        chunk = await resp.content.read(n)
        buffer += chunk
        count += len(chunk)

        if count == content_length:
            break
    return buffer
