"""Batch parsing helpers for protocol v1"""

from __future__ import annotations

from functools import partial
from typing import AsyncIterator

from aiohttp import ClientResponse

from . import (
    BatchedRecord,
    LABEL_PREFIX,
    TIME_PREFIX,
    _read,
    _read_all,
    _read_response,
)


def make_headers_v1(items) -> tuple[int, dict[str, str]]:
    """Build headers for batch write using protocol v1"""
    record_headers: dict[str, str] = {}
    content_length = 0
    for record in items:
        content_length += record.size
        header = f"{record.size},{record.content_type}"
        for label, value in record.labels.items():
            if "," in label or "=" in label:
                header += f',{label}="{value}"'
            else:
                header += f",{label}={value}"

        record_headers[f"{TIME_PREFIX}{record.timestamp}"] = header

    record_headers["Content-Type"] = "application/octet-stream"
    return content_length, record_headers


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


async def parse_batched_records_v1(resp: ClientResponse) -> AsyncIterator[BatchedRecord]:
    records_total = sum(1 for header in resp.headers if header.lower().startswith(TIME_PREFIX))
    records_count = 0
    head = resp.method == "HEAD"

    for name, value in resp.headers.items():
        if name.lower().startswith(TIME_PREFIX):
            timestamp = int(name[len(TIME_PREFIX) :])
            content_length, content_type, labels = _parse_header_as_csv_row(value)

            last = False
            records_count += 1

            if records_count == records_total:
                read_func = resp.content.iter_chunked
                read_all_func = resp.read
                if resp.headers.get("x-reduct-last", "false") == "true":
                    last = True
            else:
                if head:
                    buffer = []
                else:
                    buffer = await _read_response(resp, content_length)
                read_func = partial(_read, buffer)
                read_all_func = partial(_read_all, buffer)

            yield BatchedRecord(
                entry=resp.url.path if resp.url else "",
                timestamp=timestamp,
                size=content_length,
                last=last,
                content_type=content_type,
                labels=labels,
                read_all=read_all_func,
                read=read_func,
            )
