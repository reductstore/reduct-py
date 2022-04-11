"""Just stub for pipline"""
import random

import pytest
from aiohttp import ClientConnectionError
from reduct import Client, ServerInfo, BucketList, ReductError


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.fixture(name="bucket_name")
def _gen_bucket_name() -> str:
    return f"bucket_{random.randint(0, 1000000)}"


@pytest.fixture(name="client")
def _make_client(url):
    return Client(url)


@pytest.mark.asyncio
async def test__bad_url():
    """Should raise an error"""
    client = Client("http://127.0.0.1:65535")

    with pytest.raises(ClientConnectionError):
        await client.info()


@pytest.mark.asyncio
async def test__info(client):
    """Should get information about storage"""
    info: ServerInfo = await client.info()

    assert info.version >= "0.4.0"


@pytest.mark.asyncio
async def test__list(client, bucket_name):
    """Should browse buckets"""
    bucket_1 = bucket_name + "1"
    bucket_2 = bucket_name + "2"
    await client.create_bucket(bucket_1)
    await client.create_bucket(bucket_2)

    ret: BucketList = await client.list()

    assert len(ret.buckets) >= 2
    assert list(filter(lambda b: b.name == bucket_1, ret.buckets))[0].size == 0
    assert list(filter(lambda b: b.name == bucket_2, ret.buckets))[0].size == 0


@pytest.mark.asyncio
async def test__create_bucket(client, bucket_name):
    """Should create a new bucket"""
    bucket = await client.create_bucket(bucket_name)
    assert bucket.bucket_name == bucket_name


@pytest.mark.asyncio
async def test__create_bucket_with_error(client, bucket_name):
    """Should raise an error, if bucket exists"""
    await client.create_bucket(bucket_name)
    with pytest.raises(ReductError):
        await client.create_bucket(bucket_name)


@pytest.mark.asyncio
async def test__get_bucket(client, bucket_name):
    await client.create_bucket(bucket_name)

    bucket = await client.get_bucket(bucket_name)
    assert bucket.bucket_name == bucket_name


@pytest.mark.asyncio
async def test__get_bucket_with_error(client, bucket_name):
    """Should raise an error, if bucket doesn't exist"""
    with pytest.raises(ReductError):
        await client.get_bucket(bucket_name)
