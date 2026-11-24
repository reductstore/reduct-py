"""Unit tests for synchronous write requests."""

import pytest

from reduct import Batch, Bucket, RecordBatch


class _HttpClientStub:  # pylint: disable=too-few-public-methods
    """Capture requests without requiring a ReductStore instance."""

    api_version = (1, 22)

    def __init__(self):
        """Initialize the captured-request list."""
        self.calls = []

    async def request_all(self, method, path, **kwargs):
        """Capture a request and return a successful empty batch response."""
        self.calls.append((method, path, kwargs))
        return b"", {"x-reduct-entries": "", "x-reduct-start-ts": "0"}


def _sync_kwargs(sync: bool | None) -> dict[str, bool]:
    return {} if sync is None else {"sync": sync}


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [None, False, True], ids=["default", "false", "true"])
async def test_write_sync_header_is_opt_in(sync):
    """Single-record writes send the sync header only when requested."""
    http = _HttpClientStub()
    bucket = Bucket("bucket", http)

    await bucket.write("entry", b"data", timestamp=1, **_sync_kwargs(sync))

    method, path, kwargs = http.calls[0]
    assert (method, path) == ("POST", "/b/bucket/entry")
    if sync:
        assert kwargs["extra_headers"] == {"x-reduct-sync-write": "true"}
    else:
        assert "extra_headers" not in kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [None, False, True], ids=["default", "false", "true"])
async def test_write_batch_sync_header_is_opt_in(sync):
    """Legacy batch writes append the sync header only when requested."""
    http = _HttpClientStub()
    bucket = Bucket("bucket", http)
    batch = Batch()
    batch.add(1, b"data")

    await bucket.write_batch("entry", batch, **_sync_kwargs(sync))

    method, path, kwargs = http.calls[0]
    assert (method, path) == ("POST", "/b/bucket/entry/batch")
    headers = kwargs["extra_headers"]
    assert headers["x-reduct-time-1"] == "4,application/octet-stream"
    assert headers["Content-Type"] == "application/octet-stream"
    if sync:
        assert headers["x-reduct-sync-write"] == "true"
    else:
        assert "x-reduct-sync-write" not in headers


@pytest.mark.asyncio
@pytest.mark.parametrize("sync", [None, False, True], ids=["default", "false", "true"])
async def test_write_record_batch_sync_header_is_opt_in(sync):
    """Multi-entry batch writes append the sync header only when requested."""
    http = _HttpClientStub()
    bucket = Bucket("bucket", http)
    batch = RecordBatch()
    batch.add("entry", 1, b"data")

    await bucket.write_record_batch(batch, **_sync_kwargs(sync))

    method, path, kwargs = http.calls[0]
    assert (method, path) == ("POST", "/io/bucket/write")
    headers = kwargs["extra_headers"]
    if sync:
        assert headers["x-reduct-sync-write"] == "true"
    else:
        assert "x-reduct-sync-write" not in headers
