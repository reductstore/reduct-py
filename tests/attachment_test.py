"""Tests for attachments"""

import base64

import pytest

from tests.conftest import requires_api


@pytest.mark.asyncio
@requires_api("1.19")
async def test_write_read_attachments(bucket_1):
    """Should write and read entry attachments"""
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


@pytest.mark.asyncio
@requires_api("1.19")
async def test_remove_attachments_with_numeric_keys(bucket_1):
    """Should write and read entry attachments"""
    await bucket_1.write_attachments(
        "entry-1",
        {
            "1": {"enabled": True, "values": [1, 2, 3]},
            "2.5": {"name": "test"},
        },
    )

    attachments = await bucket_1.read_attachments("entry-1")
    assert attachments == {
        "1": {"enabled": True, "values": [1, 2, 3]},
        "2.5": {"name": "test"},
    }

    await bucket_1.remove_attachments("entry-1", attachment_keys=["1", "2.5"])
    attachments = await bucket_1.read_attachments("entry-1")
    assert attachments == {}


@pytest.mark.asyncio
@requires_api("1.19")
async def test_remove_selected_attachments(bucket_1):
    """Should remove selected attachments from an entry"""
    await bucket_1.write_attachments(
        "entry-1",
        {
            "meta-1": {"value": 1},
            "meta-2": {"value": 2},
            "$system": {"value": "test"},
        },
    )

    await bucket_1.remove_attachments("entry-1", ["meta-1", "$system"])
    attachments = await bucket_1.read_attachments("entry-1")

    assert attachments == {"meta-2": {"value": 2}}


@pytest.mark.asyncio
@requires_api("1.19")
async def test_remove_all_attachments(bucket_1):
    """Should remove all attachments from an entry"""
    await bucket_1.write_attachments(
        "entry-1",
        {
            "meta-1": {"value": 1},
            "meta-2": {"value": 2},
        },
    )

    await bucket_1.remove_attachments("entry-1")
    attachments = await bucket_1.read_attachments("entry-1")

    assert attachments == {}


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
