import pytest

from reduct.batch.batch_v2 import make_headers_v2
from reduct.record import Batch


def test_make_headers_v2_uses_entries_from_records():
    batch = Batch()
    batch.add_with_entry("entry-1", 2000, b"BB", content_type="text/plain")
    batch.add_with_entry("entry-0", 1000, b"A", content_type="text/plain")

    content_length, headers = make_headers_v2(None, batch)

    assert content_length == 3
    assert headers["x-reduct-entries"] == "entry-0,entry-1"
    assert headers["x-reduct-start-ts"] == "1000"
    assert headers["x-reduct-0-0"].startswith("1,text/plain")
    assert headers["x-reduct-1-1000"].startswith("2,text/plain")


def test_make_headers_v2_uses_default_entry_name():
    batch = Batch()
    batch.add_with_entry(None, 1000, b"A", content_type="text/plain")
    batch.add_with_entry(None, 2000, b"BB", content_type="text/plain")

    content_length, headers = make_headers_v2("entry-0", batch)

    assert content_length == 3
    assert headers["x-reduct-entries"] == "entry-0"
    assert headers["x-reduct-start-ts"] == "1000"
    assert headers["x-reduct-0-0"].startswith("1,text/plain")
    assert headers["x-reduct-0-1000"].startswith("2,text/plain")


def test_make_headers_v2_requires_entry_name_for_empty_batch():
    batch = Batch()

    with pytest.raises(ValueError, match="Entry name is required"):
        make_headers_v2(None, batch)


def test_make_headers_v2_requires_entry_name_for_record():
    batch = Batch()
    batch.add_with_entry(None, 1000, b"A", content_type="text/plain")

    with pytest.raises(ValueError, match="Entry name is required"):
        make_headers_v2(None, batch)


def test_make_headers_v2_builds_label_deltas():
    batch = Batch()
    batch.add_with_entry(
        "entry-0",
        1000,
        b"A",
        content_type="text/plain",
        labels={"a": "1", "b": "2"},
    )
    batch.add_with_entry(
        "entry-0",
        2000,
        b"B",
        content_type="text/plain",
        labels={"a": "1", "b": "3", "c": "4"},
    )
    batch.add_with_entry(
        "entry-0",
        3000,
        b"C",
        content_type="text/plain",
        labels={"a": "1", "c": "4"},
    )

    _, headers = make_headers_v2(None, batch)

    assert headers["x-reduct-entries"] == "entry-0"
    assert headers["x-reduct-labels"] == "a,b,c"
    assert headers["x-reduct-0-0"].startswith("1,text/plain,0=1,1=2")
    assert headers["x-reduct-0-1000"].startswith("1,,1=3,2=4")
    assert headers["x-reduct-0-2000"].startswith("1,,1=")
