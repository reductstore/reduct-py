"""Just stub for pipline"""
import random
from asyncio import sleep
from typing import List

import pytest
from aiohttp import ClientConnectionError

from reduct import Client, ServerInfo, BucketList, ReductError, BucketInfo, Bucket


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.fixture(name="client")
def _make_client(url):
    client = Client(url)
    return client


@pytest.fixture(name="bucket_1")
async def _bucket_1(client) -> Bucket:
    bucket = await client.create_bucket("bucket-1")
    await bucket.write("entry-1", b"some-data-1", timestamp=1)
    await bucket.write("entry-1", b"some-data-2", timestamp=2)
    await bucket.write("entry-2", b"some-data-3", timestamp=3)
    await bucket.write("entry-2", b"some-data-4", timestamp=4)
    yield bucket
    await bucket.remove()


@pytest.fixture(name="bucket_2")
async def _bucket_2(client) -> Bucket:
    bucket = await client.create_bucket("bucket-2")
    await bucket.write("entry-1", b"some-data-1", timestamp=5)
    await bucket.write("entry-1", b"some-data-2", timestamp=6)
    yield bucket
    await bucket.remove()


@pytest.mark.asyncio
async def test__bad_url():
    """Should raise an error"""
    client = Client("http://127.0.0.1:65535")

    with pytest.raises(ClientConnectionError):
        await client.info()


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__info(client):
    """Should get information about storage"""

    await sleep(1)

    info: ServerInfo = await client.info()
    assert info.version >= "0.4.0"
    assert info.uptime >= 1
    assert info.bucket_count == 2
    assert info.usage == 66
    assert info.oldest_record == 1_000_000
    assert info.latest_record == 6_000_000


@pytest.mark.asyncio
async def test__list(client, bucket_1, bucket_2):
    """Should browse buckets"""
    buckets: List[BucketInfo] = await client.list()

    assert len(buckets) == 2
    assert buckets[0].dict() == dict(
        name="bucket-1",
        entry_count=2,
        size=44,
        oldest_record=1_000_000,
        latest_record=4_000_000,
    )
    assert buckets[1].dict() == dict(
        name="bucket-2",
        entry_count=1,
        size=22,
        oldest_record=5_000_000,
        latest_record=6_000_000,
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
async def test__create_bucket_with_error(client):
    """Should raise an error, if bucket exists"""
    with pytest.raises(ReductError):
        await client.create_bucket("bucket-1")


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
async def test__get_bucket(client):
    """Should get a bucket by name"""
    bucket = await client.get_bucket("bucket-1")
    assert bucket.name == "bucket-1"


@pytest.mark.asyncio
async def test__get_bucket_with_error(client):
    """Should raise an error, if bucket doesn't exist"""
    with pytest.raises(ReductError):
        await client.get_bucket("NOTEXIST")
