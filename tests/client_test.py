"""Tests for Client"""
from asyncio import sleep
from typing import List

import pytest
from aiohttp import ClientConnectionError

from reduct import (
    Client,
    ServerInfo,
    ReductError,
    BucketInfo,
    QuotaType,
    BucketSettings,
)


@pytest.mark.asyncio
async def test__bad_url():
    """Should raise an error"""
    client = Client("http://127.0.0.1:65535")

    with pytest.raises(ClientConnectionError):
        await client.info()

@pytest.mark.asyncio
async def test__bad_url_server_exists():
    """Should raise an error"""
    client = Client("http://example.com")

    with pytest.raises(ReductError):
        await client.info()


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__info(client):
    """Should get information about storage"""

    await sleep(1)

    info: ServerInfo = await client.info()
    assert info.version >= "0.7.0"
    assert info.uptime >= 1
    assert info.bucket_count == 2
    assert info.usage == 66
    assert info.oldest_record == 1_000_000
    assert info.latest_record == 6_000_000

    assert info.defaults.bucket.dict() == {
        "max_block_size": 64000000,
        "max_block_records": 1024,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


@pytest.mark.asyncio
async def test__list(client, bucket_1, bucket_2):
    """Should browse buckets"""
    buckets: List[BucketInfo] = await client.list()

    assert len(buckets) == 2
    assert buckets[0] == await bucket_1.info()
    assert buckets[1] == await bucket_2.info()


@pytest.mark.asyncio
async def test__create_bucket_default_settings(bucket_1):
    """Should create a bucket with default settings"""
    settings = await bucket_1.get_settings()
    assert settings.dict() == {
        "max_block_size": 64000000,
        "max_block_records": 1024,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


@pytest.mark.asyncio
async def test__creat_bucket_exist_ok(client, bucket_1):
    """Should raise not raise error, if bucket exists"""
    bucket = await client.create_bucket(bucket_1.name, exist_ok=True)
    assert await bucket.info() == await bucket_1.info()


@pytest.mark.asyncio
async def test__create_bucket_custom_settings(client):
    """Should create a bucket with custom settings"""
    bucket = await client.create_bucket("bucket", BucketSettings(max_block_size=10000))
    settings = await bucket.get_settings()
    assert settings.dict() == {
        "max_block_size": 10000,
        "max_block_records": 1024,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


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
    with pytest.raises(ReductError) as reduct_err:
        await client.get_bucket("NOTEXIST")
    assert "Status 404: Bucket 'NOTEXIST' is not found" == str(reduct_err.value)


def test__exception_formatting():
    """Check the output formatting of raised exceptions"""
    with pytest.raises(ReductError, match="Status 404: Not Found"):
        raise ReductError(404, '{"detail":"Not Found"}')
