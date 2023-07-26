"""Tests for Client"""
from asyncio import sleep
from typing import List

import pytest
import pytest_asyncio

from reduct import (
    Client,
    ServerInfo,
    ReductError,
    BucketInfo,
    QuotaType,
    BucketSettings,
    Permissions,
    FullTokenInfo,
)
from .conftest import requires_env


@pytest_asyncio.fixture(name="with_token")
async def _create_token(client):
    """Create a token for tests"""
    _ = await client.create_token(
        "test-token",
        Permissions(full_access=True, read=["bucket-1"], write=["bucket-2"]),
    )
    yield "test-token"
    try:
        await client.remove_token("test-token")
    except ReductError:
        pass


@pytest.mark.asyncio
async def test__bad_url():
    """Should raise an error"""
    client = Client("http://127.0.0.1:65535")

    with pytest.raises(ReductError, match="Connection failed"):
        await client.info()


@pytest.mark.asyncio
async def test__bad_url_server_exists():
    """Should raise an error"""
    client = Client("http://127.0.0.1:8383/bad-path")

    with pytest.raises(ReductError) as reduct_err:
        await client.info()
    assert str(reduct_err.value) == ("Status 404: Not found")


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__info(client):
    """Should get information about storage"""

    await sleep(1)

    info: ServerInfo = await client.info()
    assert info.version >= "1.2.0"
    assert info.uptime >= 1
    assert info.bucket_count == 2
    assert info.usage == 66
    assert info.oldest_record == 1_000_000
    assert info.latest_record == 6_000_000

    defaults = info.defaults.bucket.dict()
    assert defaults["max_block_size"] == 64000000
    assert defaults["max_block_records"] >= 256  # defaults are different in 1.6.0
    assert defaults["quota_size"] == 0
    assert defaults["quota_type"] == QuotaType.NONE


@pytest.mark.asyncio
async def test__list(client, bucket_1, bucket_2):
    """Should browse buckets"""
    buckets: List[BucketInfo] = await client.list()

    assert len(buckets) == 2
    assert buckets[0] == await bucket_1.info()
    assert buckets[1] == await bucket_2.info()


@pytest.mark.asyncio
async def test__create_bucket_default_settings(client, bucket_1):
    """Should create a bucket with default settings"""
    settings = await bucket_1.get_settings()
    assert settings.dict() == (await client.info()).defaults.bucket.dict()


@pytest.mark.asyncio
async def test__creat_bucket_exist_ok(client, bucket_1):
    """Should raise not raise error, if bucket exists"""
    bucket = await client.create_bucket(bucket_1.name, exist_ok=True)
    assert await bucket.info() == await bucket_1.info()


@pytest.mark.asyncio
async def test__create_bucket_custom_settings(client):
    """Should create a bucket with custom settings"""
    bucket = await client.create_bucket(
        "bucket", BucketSettings(max_block_records=10000)
    )
    settings = await bucket.get_settings()
    assert settings.dict() == {
        "max_block_size": 64000000,
        "max_block_records": 10000,
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
        raise ReductError(404, "Not Found")


@requires_env("RS_API_TOKEN")
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@pytest.mark.asyncio
async def test__create_token(client):
    """Should create a token"""
    token = await client.create_token(
        "test-token",
        Permissions(full_access=True, read=["bucket-1"], write=["bucket-2"]),
    )
    assert "test-token-" in token


@requires_env("RS_API_TOKEN")
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@pytest.mark.asyncio
async def test__create_token_with_error(client, with_token):
    """Should raise an error, if token exists"""
    with pytest.raises(
        ReductError, match="Status 409: Token 'test-token' already exists"
    ):
        await client.create_token(
            with_token, Permissions(full_access=True, read=[], write=[])
        )


@requires_env("RS_API_TOKEN")
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@pytest.mark.asyncio
async def test__get_token(client, with_token):
    """Should get a token by name"""
    token = await client.get_token(with_token)
    assert token.name == with_token
    assert token.permissions.dict() == {
        "full_access": True,
        "read": ["bucket-1"],
        "write": ["bucket-2"],
    }


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__get_token_with_error(client):
    """Should raise an error, if token doesn't exist"""
    with pytest.raises(ReductError, match="Status 404: Token 'NOTEXIST' doesn't exist"):
        await client.get_token("NOTEXIST")


@requires_env("RS_API_TOKEN")
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@pytest.mark.asyncio
async def test__list_tokens(client, with_token):
    """Should list all tokens"""
    tokens = await client.get_token_list()
    assert len(tokens) == 2
    assert tokens[1].name == with_token
    assert tokens[1].created_at is not None


@requires_env("RS_API_TOKEN")
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@pytest.mark.asyncio
async def test__remove_token(client, with_token):
    """Should delete a token"""
    await client.remove_token(with_token)
    with pytest.raises(
        ReductError, match="Status 404: Token 'test-token' doesn't exist"
    ):
        await client.get_token(with_token)


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__me(client):
    """Should get user info"""
    current_token: FullTokenInfo = await client.me()
    assert current_token.name == "init-token"
    assert current_token.permissions.dict() == {
        "full_access": True,
        "read": [],
        "write": [],
    }


@pytest.mark.asyncio
async def test__with(url, api_token):
    async with Client(url, api_token=api_token) as client:
        bucket = await client.create_bucket("bucket-1", exist_ok=True)
        await bucket.info()
