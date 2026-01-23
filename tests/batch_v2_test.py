"""Tests for batch v2 headers."""

import pytest

from reduct.batch.batch_v2 import RecordBatch, make_headers_v2, _parse_batched_headers


def test_make_headers_v2_uses_entries_from_records():
    """Build headers using entries discovered from records."""
    batch = RecordBatch()
    batch.add("entry-1", 2000, b"BB", content_type="text/plain")
    batch.add("entry-0", 1000, b"A", content_type="text/plain")

    content_length, headers = make_headers_v2(batch)

    assert content_length == 3
    assert headers["x-reduct-entries"] == "entry-0,entry-1"
    assert headers["x-reduct-start-ts"] == "1000"
    assert headers["x-reduct-0-0"].startswith("1,text/plain")
    assert headers["x-reduct-1-1000"].startswith("2,text/plain")


def test_make_headers_v2_uses_single_entry_name():
    """Use the single entry name for all records in the batch."""
    batch = RecordBatch()
    batch.add("entry-0", 1000, b"A", content_type="text/plain")
    batch.add("entry-0", 2000, b"BB", content_type="text/plain")

    content_length, headers = make_headers_v2(batch)

    assert content_length == 3
    assert headers["x-reduct-entries"] == "entry-0"
    assert headers["x-reduct-start-ts"] == "1000"
    assert headers["x-reduct-0-0"].startswith("1,text/plain")
    assert headers["x-reduct-0-1000"].startswith("2")


def test_make_headers_v2_handles_empty_batch():
    """Return default headers for an empty batch."""
    batch = RecordBatch()

    content_length, headers = make_headers_v2(batch)

    assert content_length == 0
    assert headers["x-reduct-entries"] == ""
    assert headers["x-reduct-start-ts"] == "0"


def test_make_headers_v2_requires_entry_name_for_record():
    """Reject records without entry names."""
    batch = RecordBatch()
    batch.add(None, 1000, b"A", content_type="text/plain")

    with pytest.raises(ValueError, match="Entry name is required"):
        make_headers_v2(batch)


def test_make_headers_v2_builds_label_deltas():
    """Build label deltas across records."""
    batch = RecordBatch()
    batch.add(
        "entry-0",
        1000,
        b"A",
        content_type="text/plain",
        labels={"a": "1", "b": "2"},
    )
    batch.add(
        "entry-0",
        2000,
        b"B",
        content_type="text/plain",
        labels={"a": "1", "b": "3", "c": "4"},
    )
    batch.add(
        "entry-0",
        3000,
        b"C",
        content_type="text/plain",
        labels={"a": "1", "c": "4"},
    )

    _, headers = make_headers_v2(batch)

    assert headers["x-reduct-entries"] == "entry-0"
    assert headers["x-reduct-labels"] == "a,b,c"
    assert headers["x-reduct-0-0"].startswith("1,text/plain,0=1,1=2")
    assert headers["x-reduct-0-1000"].startswith("1,,1=3,2=4")
    assert headers["x-reduct-0-2000"].startswith("1,,1=")


def test_parse_empty_batch_headers():
    """Parse headers for an empty batch without raising an error."""
    headers = {
        "x-reduct-entries": "",
        "x-reduct-start-ts": "0",
    }

    parsed_headers = _parse_batched_headers(headers)

    assert not parsed_headers
