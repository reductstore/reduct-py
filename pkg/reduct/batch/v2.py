"""Batch parsing helpers for protocol v2 (multi-entry)"""

from __future__ import annotations

import urllib.parse
from functools import partial
from typing import AsyncIterator

from aiohttp import ClientResponse

from . import (
    BatchedRecord,
    ENTRIES_HEADER,
    ERROR_PREFIX,
    LABELS_HEADER,
    START_TS_HEADER,
    _read,
    _read_all,
    _read_response,
)


def _encode_header_value(value: str) -> str:
    if "," in value or "=" in value:
        return f'"{value}"'
    return value


def _encode_list(items: list[str]) -> str:
    return ",".join(urllib.parse.quote(item, safe="") for item in items)


def _labels_delta(
    previous: dict[str, str], current: dict[str, str], label_index: dict[str, int]
) -> str | None:
    ops: list[tuple[str, str | None]] = []
    for name in sorted(current.keys()):
        value = current[name]
        if name not in previous or previous[name] != value:
            ops.append((name, value))

    for name in sorted(previous.keys()):
        if name not in current:
            ops.append((name, None))

    if not ops:
        return None

    encoded = []
    for name, value in ops:
        key = str(label_index[name]) if name in label_index else name
        if value is None:
            encoded.append(f"{key}=")
        else:
            encoded.append(f"{key}={_encode_header_value(value)}")

    return ",".join(encoded)


def make_headers_v2(items) -> tuple[int, dict[str, str]]:
    """Build headers for batch write using protocol v2 (multi-entry)."""
    if not items:
        return 0, {"Content-Type": "application/octet-stream"}

    headers: dict[str, str] = {}
    content_length = 0

    entries: list[str] = []
    entry_index: dict[str, int] = {}
    for item in items:
        if item.entry not in entry_index:
            entry_index[item.entry] = len(entries)
            entries.append(item.entry)

    headers[ENTRIES_HEADER] = _encode_list(entries)
    start_ts = min(item.timestamp for item in items)
    headers[START_TS_HEADER] = str(start_ts)

    label_names = sorted({name for item in items for name in item.labels})
    label_index = {name: idx for idx, name in enumerate(label_names)}
    if label_names:
        headers[LABELS_HEADER] = _encode_list(label_names)

    last_header: dict[int, tuple[str, dict[str, str]]] = {}
    for record in items:
        content_length += record.size
        entry_idx = entry_index[record.entry]
        delta = record.timestamp - start_ts
        prev = last_header.get(entry_idx)
        prev_content_type, prev_labels = prev if prev else ("", {})

        if record.content_type == "":
            content_type = ""
        elif record.content_type:
            content_type = record.content_type
        elif prev_content_type:
            content_type = prev_content_type
        else:
            content_type = "application/octet-stream"
        labels_part = _labels_delta(prev_labels, record.labels, label_index)

        header_value = f"{record.size},{content_type}"
        if labels_part:
            header_value += f",{labels_part}"

        headers[f"x-reduct-{entry_idx}-{delta}"] = header_value
        last_header[entry_idx] = (content_type, record.labels)

    headers["Content-Type"] = "application/octet-stream"
    return content_length, headers


def _parse_encoded_list(raw: str) -> list[str]:
    entries = []
    for item in raw.split(","):
        trimmed = item.strip()
        if not trimmed:
            continue
        entries.append(urllib.parse.unquote(trimmed))
    return entries


def _parse_label_delta_ops(raw: str, label_names: list[str] | None) -> list[tuple[str, str | None]]:
    ops: list[tuple[str, str | None]] = []
    pos = 0
    length = len(raw)

    def _map_key(key: str) -> str:
        if label_names is not None and key.isdigit():
            idx = int(key)
            if 0 <= idx < len(label_names):
                return label_names[idx]
        return key

    while pos < length:
        while pos < length and raw[pos].isspace():
            pos += 1
        if pos >= length:
            break

        eq = raw.find("=", pos)
        if eq == -1:
            break
        key = _map_key(raw[pos:eq].strip())
        pos = eq + 1

        value: str | None = None
        if pos < length and raw[pos] == '"':
            pos += 1
            end_quote = raw.find('"', pos)
            if end_quote == -1:
                break
            value = raw[pos:end_quote]
            pos = end_quote + 1
        else:
            comma = raw.find(",", pos)
            segment = raw[pos : comma if comma != -1 else length]
            segment = segment.strip()
            value = segment if segment else None
            pos = length if comma == -1 else comma

        ops.append((key, value))
        if pos < length and raw[pos] == ",":
            pos += 1

    return ops


def _apply_label_delta(raw: str, base: dict[str, str], label_names: list[str] | None) -> dict[str, str]:
    labels = dict(base)
    for key, value in _parse_label_delta_ops(raw, label_names):
        if value is None:
            labels.pop(key, None)
        else:
            labels[key] = value
    return labels


def _parse_record_header_v2(
    raw: str, previous: tuple[int, str, dict[str, str]] | None, label_names: list[str] | None
) -> tuple[int, str, dict[str, str]] | None:
    first_comma = raw.find(",")
    length_str = raw if first_comma == -1 else raw[:first_comma]
    length_str = length_str.strip()
    if not length_str:
        return None
    try:
        content_length = int(length_str)
    except ValueError:
        return None

    rest = raw[first_comma + 1 :] if first_comma != -1 else ""
    second_comma = rest.find(",") if rest else -1
    if second_comma == -1:
        content_type = rest.strip() if rest else ""
        labels_raw = None
    else:
        content_type = rest[:second_comma].strip()
        labels_raw = rest[second_comma + 1 :]

    if not content_type:
        content_type = previous[1] if previous else "application/octet-stream"

    labels = previous[2] if previous else {}
    if labels_raw is not None:
        labels = _apply_label_delta(labels_raw, labels, label_names)

    return content_length, content_type, labels


async def parse_batched_records_v2(resp: ClientResponse) -> AsyncIterator[BatchedRecord] | None:
    entries_raw = resp.headers.get(ENTRIES_HEADER)
    start_ts_raw = resp.headers.get(START_TS_HEADER)
    if entries_raw is None or start_ts_raw is None:
        return None

    try:
        start_ts = int(start_ts_raw)
    except ValueError:
        return None

    entries = _parse_encoded_list(entries_raw)
    if not entries:
        return None

    label_names = None
    labels_raw = resp.headers.get(LABELS_HEADER)
    if labels_raw:
        label_names = _parse_encoded_list(labels_raw)

    parsed_headers: list[tuple[int, int, str]] = []
    for name, value in resp.headers.items():
        low = name.lower()
        if not low.startswith("x-reduct-") or low.startswith(ERROR_PREFIX):
            continue
        suffix = low[len("x-reduct-") :]
        dash = suffix.rfind("-")
        if dash == -1:
            continue
        entry_part = suffix[:dash]
        delta_part = suffix[dash + 1 :]
        if not entry_part.isdigit():
            continue
        try:
            entry_idx = int(entry_part)
            delta = int(delta_part)
        except ValueError:
            continue
        parsed_headers.append((entry_idx, delta, value))

    parsed_headers.sort(key=lambda item: (item[0], item[1]))
    if not parsed_headers:
        return None

    last_flag = resp.headers.get("x-reduct-last", "false").lower() == "true"
    last_header: dict[int, tuple[int, str, dict[str, str]] | None] = {}

    async def _iter_records() -> AsyncIterator[BatchedRecord]:
        total_records = len(parsed_headers)
        for idx, (entry_idx, delta, raw_value) in enumerate(parsed_headers):
            if entry_idx >= len(entries):
                continue

            header = _parse_record_header_v2(raw_value, last_header.get(entry_idx), label_names)
            if header is None:
                continue

            last_header[entry_idx] = header
            timestamp = start_ts + delta
            content_length, content_type, labels = header

            if idx == total_records - 1:
                read_func = resp.content.iter_chunked
                read_all_func = resp.read
            else:
                if resp.method == "HEAD":
                    buffer = []
                else:
                    buffer = await _read_response(resp, content_length)
                read_func = partial(_read, buffer)
                read_all_func = partial(_read_all, buffer)

            yield BatchedRecord(
                entry=entries[entry_idx],
                timestamp=timestamp,
                size=content_length,
                last=last_flag and idx == total_records - 1,
                content_type=content_type,
                labels=labels,
                read_all=read_all_func,
                read=read_func,
            )

    return _iter_records()
