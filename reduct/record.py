"""Record representation and its parsing"""
import asyncio
from dataclasses import dataclass
from functools import partial
from typing import Dict, Callable, AsyncIterator, Awaitable

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


LABEL_PREFIX = "x-reduct-label-"


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


def _parse_csv_row(row: str):
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

    data = {"labels": {}}
    for item in items:
        kv_pair = item.split("=", 1)

        if kv_pair[0].startswith("label-"):
            data["labels"][kv_pair[0][6:]] = kv_pair[1]
        else:
            data[kv_pair[0]] = kv_pair[1]

    return data


async def parse_batched_records(resp: ClientResponse) -> AsyncIterator[Record]:
    """Parse batched records from response"""

    records_total = sum(
        1 for header in resp.headers if header.startswith("x-reduct-time-")
    )
    records_count = 0
    read_counter = [0]
    global_offset = 0

    async def read(offset, size, n):
        if read_counter[0] != offset:
            raise RuntimeError(
                f"Read batched records out of order: {read_counter[0]} != {offset}"
            )
        count = 0
        n = min(n, size)

        while True:
            chunk = await resp.content.read(n)
            read_counter[0] += len(chunk)
            count += len(chunk)
            n = min(n, size - count)
            yield chunk

            if count == size:
                break

    async def read_all(offset, size):
        data = b""
        async for chunk in read(offset, size, 512_000):
            data += chunk
        return data

    for name, value in resp.headers.items():
        if name.startswith("x-reduct-time-"):
            timestamp = int(name[14:])
            meta_data = _parse_csv_row(value)
            content_length = int(meta_data["content-length"])

            record = Record(
                timestamp=timestamp,
                size=content_length,
                last=False,
                content_type=meta_data["content-type"],
                labels=meta_data["labels"],
                read_all=partial(read_all, global_offset, content_length),
                read=partial(read, global_offset, content_length),
            )

            global_offset += content_length
            records_count += 1
            if records_count == records_total:
                if resp.headers.get("x-reduct-last", "false") == "true":
                    record.last = True

            yield record
            await asyncio.sleep(0)
