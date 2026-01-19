import asyncio
from functools import partial
from typing import AsyncIterator

from aiohttp import ClientResponse

from reduct.error import ReductError
from reduct.record import Record, Batch, ERROR_PREFIX

TIME_PREFIX = "x-reduct-time-"


CHUNK_SIZE = 16_000


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


async def read_all(buffer: list[bytes]) -> bytes:
    return b"".join(buffer)


async def read_response(resp, content_length) -> list[bytes]:
    chunks = []
    count = 0
    while count < content_length:
        n = min(CHUNK_SIZE, content_length - count)
        chunk = await resp.content.read(n)
        chunks.append(chunk)
        count += len(chunk)

    return chunks


def _parse_header_as_csv_row(row: str) -> tuple[int, str, dict[str, str]]:
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


async def parse_batched_records_v1(
    resp: ClientResponse, default_entry_name: str
) -> AsyncIterator[Record]:
    """Parse batched records from response"""

    records_total = sum(
        1 for header in resp.headers if header.lower().startswith(TIME_PREFIX)
    )
    records_count = 0
    head = resp.method == "HEAD"

    for name, value in resp.headers.items():
        if name.lower().startswith(TIME_PREFIX):
            timestamp = int(name[len(TIME_PREFIX) :])
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
                    buffer = []
                else:
                    buffer = await read_response(resp, content_length)
                read_func = partial(_read, buffer)
                read_all_func = partial(read_all, buffer)

            record = Record(
                timestamp=timestamp,
                entry=default_entry_name,
                size=content_length,
                last=last,
                content_type=content_type,
                labels=labels,
                read_all=read_all_func,
                read=read_func,
            )

            yield record


def make_headers_v1(batch: Batch) -> tuple[int, dict[str, str]]:
    """Make headers for batch"""
    record_headers = {}
    content_length = 0
    for meta, record in batch.items():
        time_stamp = meta[1]
        content_length += record.size
        header = f"{record.size},{record.content_type}"
        for label, value in record.labels.items():
            if "," in label or "=" in label:
                header += f',{label}="{value}"'
            else:
                header += f",{label}={value}"

        record_headers[f"{TIME_PREFIX}{time_stamp}"] = header

    record_headers["Content-Type"] = "application/octet-stream"
    return content_length, record_headers


def parse_errors_from_headers_v1(headers):
    errors = {}
    for key, value in headers.items():
        if key.startswith(ERROR_PREFIX):
            errors[int(key[len(ERROR_PREFIX) :])] = ReductError.from_header(value)
    return errors
