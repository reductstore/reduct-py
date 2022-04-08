"""Just stub for pipline"""
import random

import pytest
from reduct import Client, ServerInfo, BucketList


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.fixture(name="bucket_name")
def _gen_bucket_name() -> str:
    return f"bucket_{random.randint(0, 1000000)}"


@pytest.mark.asyncio
async def test__info(url):
    """Should get information about storage"""
    client = Client(url)
    info: ServerInfo = await client.info()

    assert info.version >= "0.4.0"


@pytest.mark.asyncio
async def test__list(url, bucket_name):
    """Should browse buckets"""
    client = Client(url)
    bucket_1 = bucket_name + "1"
    bucket_2 = bucket_name + "2"
    await client.create_bucket(bucket_1)
    await client.create_bucket(bucket_2)

    ret: BucketList = await client.list()

    assert len(ret.buckets) >= 2
    assert list(filter(lambda b: b.name == bucket_1, ret.buckets))[0].size == 0
    assert list(filter(lambda b: b.name == bucket_2, ret.buckets))[0].size == 0
