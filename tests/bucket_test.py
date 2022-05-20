"""Tests for Bucket"""
import time

import pytest

from reduct import ReductError, BucketSettings


@pytest.mark.asyncio
async def test__remove_ok(client):
    """Should remove a bucket"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await client.get_bucket("bucket")


@pytest.mark.asyncio
async def test__remove_not_exist(client):
    """Should not remove a bucket if it doesn't exist"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await bucket.remove()


@pytest.mark.asyncio
async def test__set_settings(bucket_1):
    """Should set new settings"""
    await bucket_1.set_settings(BucketSettings(max_block_size=10000))
    new_settings = await bucket_1.get_settings()
    assert new_settings.dict() == {
        "max_block_size": 10000,
        "quota_size": None,
        "quota_type": None,
    }  # actually bug


@pytest.mark.asyncio
async def test__get_info(bucket_2):
    """Should get info about bucket"""
    info = await bucket_2.info()
    assert info.dict() == {
        "entry_count": 1,
        "latest_record": 6000000,
        "name": "bucket-2",
        "oldest_record": 5000000,
        "size": 22,
    }


@pytest.mark.asyncio
async def test__get_entries(bucket_1):
    """Should get list of entries"""
    entries = await bucket_1.get_entry_list()
    assert len(entries) == 2
    assert entries[0].dict() == {
        "block_count": 1,
        "latest_record": 2000000,
        "name": "entry-1",
        "oldest_record": 1000000,
        "record_count": 2,
        "size": 22,
    }

    assert entries[1].dict() == {
        "block_count": 1,
        "latest_record": 4000000,
        "name": "entry-2",
        "oldest_record": 3000000,
        "record_count": 2,
        "size": 22,
    }


@pytest.mark.asyncio
async def test__read_by_timestamp(bucket_1):
    """Should read a record by timestamp"""
    data = await bucket_1.read("entry-2", timestamp=3_000_000)
    assert data == b"some-data-3"


@pytest.mark.asyncio
async def test__read_latest(bucket_1):
    """Should read the latest record if no timestamp"""
    data = await bucket_1.read("entry-2")
    assert data == b"some-data-4"


@pytest.mark.asyncio
async def test__write_by_timestamp(bucket_2):
    """Should write a record by timestamp"""
    await bucket_2.write("entry-3", b"test-data", timestamp=5_000_000)
    data = await bucket_2.read("entry-3", timestamp=5_000_000)
    assert data == b"test-data"


@pytest.mark.asyncio
async def test__write_with_current_time(bucket_2):
    """Should write a record with current time"""
    belated_timestamp = int(time.time_ns() / 1000)

    await bucket_2.write("entry-3", b"test-data")
    await bucket_2.write("entry-3", b"old-data", timestamp=belated_timestamp)
    data = await bucket_2.read("entry-3")
    assert data == b"test-data"
