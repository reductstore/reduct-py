"""Tests for non-JSON attachments"""

import base64

import pytest

from tests.conftest import requires_api


@pytest.mark.asyncio
@requires_api("1.19")
async def test_write_read_non_json_attachment(bucket_1):
    """Should write and read non-JSON attachments"""
    raw_bytes = b"\xde\xad\xbe\xef"
    encoded = base64.b64encode(raw_bytes).decode()

    await bucket_1.write_attachments(
        "entry-1",
        {"binary-data": encoded},
        content_type="application/octet-stream",
    )

    attachments = await bucket_1.read_attachments("entry-1")
    assert attachments["binary-data"] == encoded


@pytest.mark.asyncio
@requires_api("1.19")
async def test_write_attachments_default_json(bucket_1):
    """Should write and read JSON attachments with default content type"""
    await bucket_1.write_attachments(
        "entry-1",
        {
            "meta-1": {"enabled": True, "values": [1, 2, 3]},
            "meta-2": {"name": "test"},
        },
    )

    attachments = await bucket_1.read_attachments("entry-1")
    assert attachments == {
        "meta-1": {"enabled": True, "values": [1, 2, 3]},
        "meta-2": {"name": "test"},
    }
