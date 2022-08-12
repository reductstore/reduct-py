"""Tests for Bucket"""
import time
from typing import List

import pytest

from reduct import ReductError, BucketSettings, QuotaType
from reduct.bucket import Record


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
        "max_block_records": 1024,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


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
async def test__read_by_chunks(bucket_1):
    """Should read by chunks"""
    data = b""
    async for chunk in bucket_1.read_by("entry-2", chunk_size=3):
        data += chunk

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


@pytest.mark.asyncio
async def test__write_by_chunks(bucket_2):
    """Should accept interator for writing by chunks"""

    async def sender():
        for chunk in [b"part1", b"part2"]:
            yield chunk

    await bucket_2.write("entry-1", sender(), content_length=10)
    data = await bucket_2.read("entry-1")
    assert data == b"part1part2"


@pytest.mark.asyncio
async def test__list(bucket_1):
    """Should get list of records for time interval"""
    records = await bucket_1.list("entry-2", start=0, stop=5_000_000)
    assert records == [(3000000, 11), (4000000, 11)]


@pytest.mark.asyncio
async def test_query_records(bucket_1):
    """Should query records for a time interval"""
    records: List[Record] = [
        record
        async for record in bucket_1.query("entry-2", start=0, stop=5_000_000, ttl=5)
    ]
    assert len(records) == 2

    assert records[0].timestamp == 3000000
    assert records[0].size == 11
    assert not records[0].last

    assert records[1].timestamp == 4000000
    assert records[1].size == 11
    assert records[1].last


@pytest.mark.asyncio
async def test_query_records_first(bucket_1):
    """Should query records for from first record"""

    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", stop=4_000_000)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 3_000_000


@pytest.mark.asyncio
async def test_query_records_last(bucket_1):
    """Should query records for until last record"""
    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", start=4_000_000)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 4_000_000


@pytest.mark.asyncio
async def test_query_records_all(bucket_1):
    """Should query records all data"""
    records = [record async for record in bucket_1.query("entry-2")]
    assert len(records) == 2


@pytest.mark.asyncio
async def test_read_record(bucket_1):
    """Should provide records with read method"""
    data = [await record.read_all() async for record in bucket_1.query("entry-2")]
    assert data == [b"some-data-3", b"some-data-4"]

    data = []

    async for record in bucket_1.query("entry-2"):
        async for chunk in record.read(n=4):
            data.append(chunk)

    assert data == [b"some", b"-dat", b"a-3", b"some", b"-dat", b"a-4"]
