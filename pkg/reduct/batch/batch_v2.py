from dataclasses import dataclass
from functools import partial
from typing import AsyncIterator

from aiohttp import ClientResponse

from reduct.batch.common import BaseBatch, read_response, read_all, read
from reduct.error import ReductError
from reduct.record import Record
from reduct.time import TimestampLike

HEADER_PREFIX = "x-reduct-"
ERROR_HEADER_PREFIX = "x-reduct-error-"
ENTRIES_HEADER = "x-reduct-entries"
START_TS_HEADER = "x-reduct-start-ts"
LABELS_HEADER = "x-reduct-labels"

CHUNK_SIZE = 16_000


class RecordBatch(BaseBatch):
    """Batch of records to write them in one request (v1)"""

    def add(
        self,
        entry: str,
        timestamp: TimestampLike,
        data: bytes = b"",
        **kwargs,
    ):
        """Add record to batch with entry name
        Args:
            entry: entry name
            timestamp: timestamp of record. int (UNIX timestamp in microseconds),
                datetime, float (UNIX timestamp in seconds), str (ISO 8601 string)
            data: data to store

        Kwargs:
            content_type: content type of data (default: application/octet-stream)
            labels: labels of record (default: {})

        """
        super()._add(entry, timestamp, data, **kwargs)


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


def _is_tchar(byte: int) -> bool:
    return (
        48 <= byte <= 57
        or 65 <= byte <= 90
        or 97 <= byte <= 122
        or byte in b"!#$%&'*+-.^_`|~"
    )


def _encode_entry_name(entry: str) -> str:
    encoded = []
    for byte in entry.encode():
        if _is_tchar(byte):
            encoded.append(chr(byte))
        else:
            encoded.append(f"%{byte:02X}")
    return "".join(encoded)


def _format_label_value(value: str) -> str:
    if "," in value:
        return f'"{value}"'
    return value


def _build_label_delta(
    labels: dict[str, str],
    previous_labels: dict[str, str] | None,
    label_index: dict[str, int],
    label_names: list[str],
) -> str:
    ops: list[tuple[int, str]] = []

    def ensure_label(name: str) -> int:
        if name in label_index:
            return label_index[name]
        idx = len(label_names)
        label_index[name] = idx
        label_names.append(name)
        return idx

    if previous_labels is None:
        for key in sorted(labels.keys()):
            idx = ensure_label(key)
            ops.append((idx, _format_label_value(labels[key])))
    else:
        keys = set(previous_labels.keys())
        keys.update(labels.keys())
        for key in sorted(keys):
            prev_val = previous_labels.get(key)
            curr_val = labels.get(key)
            if prev_val == curr_val:
                continue
            idx = ensure_label(key)
            if curr_val is None:
                ops.append((idx, ""))
            else:
                ops.append((idx, _format_label_value(curr_val)))

    ops.sort(key=lambda item: item[0])
    return ",".join(f"{idx}={value}" for idx, value in ops)


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


def _parse_entries_header(headers: dict) -> list[str]:
    entries = headers.get(ENTRIES_HEADER, "")
    if not entries:
        raise ValueError(f"{ENTRIES_HEADER} header is required")
    return [_decode_entry_name(entry.strip()) for entry in entries.split(",")]


def _parse_start_timestamp(headers: dict) -> int:
    start_ts = headers.get(START_TS_HEADER, "")
    if not start_ts:
        raise ValueError(f"{START_TS_HEADER} header is required")
    try:
        return int(start_ts)
    except ValueError as exc:
        raise ValueError("Invalid x-reduct-start-ts header") from exc


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
    entries = _parse_entries_header(headers)
    start_ts = _parse_start_timestamp(headers)

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
            read_func = partial(read, buffer)
            read_all_func = partial(read_all, buffer)

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


def _prepare_records_v2(
    batch: RecordBatch,
) -> tuple[list[str], int, list[tuple[int, int, Record]]]:
    records = batch.items()
    if not records:
        return [], 0, []

    entries: list[str] = []
    entry_index_lookup: dict[str, int] = {}
    indexed_records: list[tuple[int, int, Record]] = []

    start_ts = min(meta[1] for meta, _ in records)

    for meta, record in records:
        timestamp = meta[1]
        record_entry = record.entry
        if record_entry is None:
            raise ValueError("Entry name is required for batch protocol v2")
        entry_index = entry_index_lookup.get(record_entry)
        if entry_index is None:
            entry_index = len(entries)
            entries.append(record_entry)
            entry_index_lookup[record_entry] = entry_index
        indexed_records.append((entry_index, timestamp, record))

    indexed_records.sort(key=lambda item: (item[0], item[1]))
    return entries, start_ts, indexed_records


def make_headers_v2(batch: RecordBatch) -> tuple[int, dict[str, str]]:
    """Make headers for batch protocol v2."""
    record_headers: dict[str, str] = {}
    content_length = 0
    records = batch.items()

    if not records:
        record_headers[ENTRIES_HEADER] = ""
        record_headers[START_TS_HEADER] = "0"
        record_headers["Content-Type"] = "application/octet-stream"
        return content_length, record_headers

    entries, start_ts, indexed_records = _prepare_records_v2(batch)
    label_index: dict[str, int] = {}
    label_names: list[str] = []

    record_headers[ENTRIES_HEADER] = ",".join(
        _encode_entry_name(entry) for entry in entries
    )
    record_headers[START_TS_HEADER] = str(start_ts)

    last_meta: dict[int, tuple[str, dict[str, str]]] = {}

    for entry_index, timestamp, record in indexed_records:
        content_length += record.size
        delta = timestamp - start_ts
        content_type = record.content_type or "application/octet-stream"
        prev_content_type = None
        prev_labels = None
        if entry_index in last_meta:
            prev_content_type, prev_labels = last_meta[entry_index]

        header_parts = [str(record.size)]
        content_type_part = ""
        if prev_content_type is None or prev_content_type != content_type:
            content_type_part = content_type

        label_delta = _build_label_delta(
            record.labels, prev_labels, label_index, label_names
        )
        has_labels = bool(label_delta)

        if content_type_part or has_labels:
            header_parts.append(content_type_part)
        if has_labels:
            header_parts.append(label_delta)

        record_headers[f"{HEADER_PREFIX}{entry_index}-{delta}"] = ",".join(header_parts)
        last_meta[entry_index] = (content_type, dict(record.labels))

    if label_names:
        record_headers[LABELS_HEADER] = ",".join(
            _encode_entry_name(name) for name in label_names
        )

    record_headers["Content-Type"] = "application/octet-stream"
    return content_length, record_headers


def parse_errors_from_headers_v2(
    headers: dict[str, str],
) -> dict[str, dict[int, ReductError]]:
    """Parse error headers for batch protocol v2."""
    errors: dict[str, dict[int, ReductError]] = {}
    entries = _parse_entries_header(headers)
    start_ts = _parse_start_timestamp(headers)
    for key, value in headers.items():
        name = key.lower()
        if not name.startswith(ERROR_HEADER_PREFIX):
            continue

        suffix = name[len(ERROR_HEADER_PREFIX) :]
        if "-" not in suffix:
            raise ValueError(f"Invalid error header '{key}'")

        entry_index_str, delta_str = suffix.rsplit("-", 1)
        entry_index = int(entry_index_str)
        delta = int(delta_str)

        entry_errr = errors.get(entries[entry_index])
        if entry_errr is None:
            entry_errr = {}
            errors[entries[entry_index]] = entry_errr

        errors[entries[entry_index]][delta + start_ts] = ReductError.from_header(value)

    return errors
