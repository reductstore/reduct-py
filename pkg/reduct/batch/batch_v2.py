import asyncio
from dataclasses import dataclass
from functools import partial
from typing import AsyncIterator

from aiohttp import ClientResponse

from reduct.record import Record


HEADER_PREFIX = "x-reduct-"
ERROR_HEADER_PREFIX = "x-reduct-error-"
ENTRIES_HEADER = "x-reduct-entries"
START_TS_HEADER = "x-reduct-start-ts"
LABELS_HEADER = "x-reduct-labels"

CHUNK_SIZE = 16_000


@dataclass
class RecordHeader:
    content_length: int
    content_type: str
    labels: dict[str, str]


@dataclass
class EntryRecordHeader:
    entry: str
    timestamp: int
    header: RecordHeader


def _decode_entry_name(encoded: str) -> str:
    decoded = bytearray()
    pos = 0
    raw = encoded.encode()
    while pos < len(raw):
        byte = raw[pos]
        if byte == ord("%"):
            if pos + 2 >= len(raw):
                raise ValueError(f"Invalid entry encoding in header name: '{encoded}'")
            try:
                decoded.append(int(raw[pos + 1 : pos + 3].decode(), 16))
            except ValueError as exc:
                raise ValueError(
                    f"Invalid entry encoding in header name: '{encoded}'"
                ) from exc
            pos += 3
        else:
            decoded.append(byte)
            pos += 1

    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"Entry name is not valid UTF-8 in header '{encoded}'"
        ) from exc


def _parse_entries_header(entries: str) -> list[str]:
    entries = entries.strip()
    if not entries:
        raise ValueError("x-reduct-entries header is required")
    return [_decode_entry_name(entry.strip()) for entry in entries.split(",")]


def _parse_labels_header(labels: str) -> list[str]:
    labels = labels.strip()
    if not labels:
        raise ValueError("x-reduct-labels header is empty")
    return [_decode_entry_name(label.strip()) for label in labels.split(",")]


def _resolve_label_name(raw: str, label_names: list[str] | None) -> str:
    if label_names is not None:
        try:
            idx = int(raw)
        except ValueError:
            idx = None
        if idx is not None:
            if idx < 0 or idx >= len(label_names):
                raise ValueError(f"Label index '{raw}' is out of range")
            return label_names[idx]

    if raw.startswith("@"):
        raise ValueError(
            "Label names must not start with '@': reserved for computed labels"
        )

    return raw


def _parse_label_delta_ops(
    raw_labels: str, label_names: list[str] | None
) -> list[tuple[str, str | None]]:
    ops: list[tuple[str, str | None]] = []
    rest = raw_labels.strip()
    if not rest:
        return ops

    while rest:
        if "=" not in rest:
            raise ValueError("Invalid batched header")
        raw_key, value_part = rest.split("=", 1)
        key = _resolve_label_name(raw_key.strip(), label_names)

        if value_part.startswith('"'):
            value_part = value_part[1:]
            if '"' not in value_part:
                raise ValueError("Invalid batched header")
            value, rest = value_part.split('"', 1)
            rest = rest.lstrip(",").strip()
        elif "," in value_part:
            value, rest = value_part.split(",", 1)
            rest = rest.strip()
        else:
            value = value_part
            rest = ""

        value = value.strip()
        ops.append((key, value if value else None))

    return ops


def _apply_label_delta(
    raw_labels: str, base: dict[str, str], label_names: list[str] | None
) -> dict[str, str]:
    labels = dict(base)
    for key, value in _parse_label_delta_ops(raw_labels, label_names):
        if value is None:
            labels.pop(key, None)
        else:
            labels[key] = value
    return labels


def _parse_record_header_with_defaults(
    raw: str, previous: RecordHeader | None, label_names: list[str] | None
) -> RecordHeader:
    if "," in raw:
        content_length_str, rest = raw.split(",", 1)
        rest = rest
    else:
        content_length_str, rest = raw, None

    try:
        content_length = int(content_length_str.strip())
    except ValueError as exc:
        raise ValueError("Invalid batched header") from exc

    if rest is None:
        if previous is None:
            raise ValueError(
                "Content-type and labels must be provided for the first record of an entry"
            )
        return RecordHeader(
            content_length=content_length,
            content_type=previous.content_type,
            labels=dict(previous.labels),
        )

    if "," in rest:
        content_type_raw, labels_raw = rest.split(",", 1)
    else:
        content_type_raw, labels_raw = rest, None

    content_type_raw = content_type_raw.strip()
    if content_type_raw:
        content_type = content_type_raw
    elif previous is not None:
        content_type = previous.content_type
    else:
        content_type = "application/octet-stream"

    if labels_raw is None:
        labels = dict(previous.labels) if previous else {}
    else:
        labels = _apply_label_delta(
            labels_raw,
            previous.labels if previous else {},
            label_names,
        )

    return RecordHeader(
        content_length=content_length, content_type=content_type, labels=labels
    )


def _parse_batched_header_name(name: str) -> tuple[int, int]:
    if not name.startswith(HEADER_PREFIX):
        raise ValueError(f"Invalid batched header '{name}'")
    without_prefix = name[len(HEADER_PREFIX) :]
    if "-" not in without_prefix:
        raise ValueError(f"Invalid batched header '{name}'")
    entry_index_str, delta_str = without_prefix.rsplit("-", 1)
    try:
        entry_index = int(entry_index_str)
    except ValueError as exc:
        raise ValueError(
            f"Invalid header '{name}': entry index must be a number"
        ) from exc
    try:
        delta = int(delta_str)
    except ValueError as exc:
        raise ValueError(
            f"Invalid header '{name}': must be an unix timestamp in microseconds"
        ) from exc
    return entry_index, delta


def _sort_headers_by_entry_and_time(
    headers: dict[str, str],
) -> list[tuple[int, int, str]]:
    parsed_headers: list[tuple[int, int, str]] = []
    for name, value in headers.items():
        lower_name = name.lower()
        if not lower_name.startswith(HEADER_PREFIX):
            continue
        if lower_name.startswith(ERROR_HEADER_PREFIX):
            continue
        if "-" not in lower_name[len(HEADER_PREFIX) :]:
            continue
        suffix = lower_name[len(HEADER_PREFIX) :]
        if "-" not in suffix:
            continue
        entry_index_str, delta_str = suffix.rsplit("-", 1)
        if not entry_index_str.isdigit() or not delta_str.isdigit():
            continue
        entry_index, delta = _parse_batched_header_name(lower_name)
        parsed_headers.append((entry_index, delta, value))

    parsed_headers.sort(key=lambda item: (item[0], item[1]))
    return parsed_headers


def _parse_batched_headers(headers: dict[str, str]) -> list[EntryRecordHeader]:
    entries_raw = headers.get(ENTRIES_HEADER)
    if entries_raw is None:
        raise ValueError("x-reduct-entries header is required")
    entries = _parse_entries_header(entries_raw)

    start_ts_raw = headers.get(START_TS_HEADER)
    if start_ts_raw is None:
        raise ValueError("x-reduct-start-ts header is required")
    try:
        start_ts = int(start_ts_raw)
    except ValueError as exc:
        raise ValueError("Invalid x-reduct-start-ts header") from exc

    labels_raw = headers.get(LABELS_HEADER)
    label_names = _parse_labels_header(labels_raw) if labels_raw else None

    last_header_per_entry: dict[str, RecordHeader] = {}
    parsed: list[EntryRecordHeader] = []

    for entry_index, delta, value in _sort_headers_by_entry_and_time(headers):
        if entry_index < 0 or entry_index >= len(entries):
            raise ValueError(
                f"Invalid header '{HEADER_PREFIX}{entry_index}-{delta}': entry index out of range"
            )
        entry = entries[entry_index]
        header = _parse_record_header_with_defaults(
            value, last_header_per_entry.get(entry), label_names
        )
        timestamp = start_ts + delta
        last_header_per_entry[entry] = header
        parsed.append(
            EntryRecordHeader(entry=entry, timestamp=timestamp, header=header)
        )

    return parsed


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


async def parse_batched_records_v2(resp: ClientResponse) -> AsyncIterator[Record]:
    """Parse batched records from response using batch protocol v2."""

    headers = {name.lower(): value for name, value in resp.headers.items()}
    parsed_headers = _parse_batched_headers(headers)
    records_total = len(parsed_headers)
    records_count = 0
    head = resp.method == "HEAD"

    for entry_header in parsed_headers:
        records_count += 1
        content_length = entry_header.header.content_length
        last = False

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

        yield Record(
            timestamp=entry_header.timestamp,
            entry=entry_header.entry,
            size=content_length,
            last=last,
            content_type=entry_header.header.content_type,
            labels=entry_header.header.labels,
            read_all=read_all_func,
            read=read_func,
        )
