"""Tests for Client"""

from asyncio import sleep
from datetime import datetime, timedelta, timezone
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
from .conftest import requires_env, requires_api


@pytest_asyncio.fixture(name="with_token")
async def _create_token(client, random_prefix):
    """Create a token for tests"""
    name = f"{random_prefix}-test-token"
    _ = await client.create_token(
        name,
        Permissions(full_access=True, read=["bucket-1"], write=["bucket-2"]),
    )
    yield name
    try:
        await client.remove_token(name)
    except ReductError:
        pass


@pytest.mark.asyncio
async def test__bad_url():
    """Should raise an error"""
    client = Client("http://127.0.0.1:65535")

    with pytest.raises(ReductError, match="Cannot connect "):
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
    assert info.version >= "1.10.0"
    assert info.uptime >= 1
    assert info.bucket_count >= 2
    assert info.usage >= 374
    assert info.oldest_record == 1_000_000
    assert info.latest_record >= 6_000_000

    defaults = info.defaults.bucket.model_dump()
    assert defaults["max_block_size"] == 64000000
    assert defaults["max_block_records"] >= 256  # defaults are different in 1.6.0
    assert defaults["quota_size"] == 0
    assert defaults["quota_type"] == QuotaType.NONE


@pytest.mark.asyncio
async def test__list(client, bucket_1, bucket_2):
    """Should browse buckets"""
    buckets: List[BucketInfo] = await client.list()

    assert len(buckets) >= 2
    assert await bucket_1.info() in buckets
    assert await bucket_2.info() in buckets


@pytest.mark.asyncio
async def test__create_bucket_default_settings(client, bucket_1):
    """Should create a bucket with default settings"""
    settings = await bucket_1.get_settings()
    assert settings.model_dump() == (await client.info()).defaults.bucket.model_dump()


@pytest.mark.asyncio
async def test__creat_bucket_exist_ok(client, bucket_1):
    """Should raise not raise error, if bucket exists"""
    bucket = await client.create_bucket(bucket_1.name, exist_ok=True)
    assert await bucket.info() == await bucket_1.info()


@pytest.mark.asyncio
async def test__create_bucket_custom_settings(client, random_prefix):
    """Should create a bucket with custom settings"""
    bucket = await client.create_bucket(
        f"{random_prefix}-bucket", BucketSettings(max_block_records=10000)
    )
    settings = await bucket.get_settings()
    assert settings.model_dump() == {
        "max_block_size": 64000000,
        "max_block_records": 10000,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


@pytest.mark.parametrize("quota_type", [QuotaType.NONE, QuotaType.FIFO, QuotaType.HARD])
@pytest.mark.asyncio
@requires_api("1.12")
async def test__create_bucket_quota(client, quota_type, random_prefix):
    """Should create a bucket with custom settings"""
    bucket = await client.create_bucket(
        f"{random_prefix}-bucket", BucketSettings(quota_type=quota_type)
    )
    settings = await bucket.get_settings()
    assert settings.model_dump()["quota_type"] == quota_type


@pytest.mark.asyncio
async def test__create_bucket_with_error(client, bucket_1):
    """Should raise an error, if bucket exists"""
    with pytest.raises(ReductError):
        await client.create_bucket(bucket_1.name)


@pytest.mark.asyncio
async def test__get_bucket(client, bucket_1):
    """Should get a bucket by name"""
    bucket = await client.get_bucket(bucket_1.name)
    assert bucket.name == bucket_1.name


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
async def test__create_token(client, random_prefix):
    """Should create a token"""
    token = await client.create_token(
        f"{random_prefix}-test-token",
        Permissions(full_access=True, read=["bucket-1"], write=["bucket-2"]),
    )
    assert "test-token-" in token


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__create_token_with_error(client, with_token, random_prefix):
    """Should raise an error, if token exists"""
    with pytest.raises(
        ReductError,
        match=f"Status 409: Token '{random_prefix}-test-token' already exists",
    ):
        await client.create_token(
            with_token, Permissions(full_access=True, read=[], write=[])
        )


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__get_token(client, with_token):
    """Should get a token by name"""
    token = await client.get_token(with_token)
    assert token.name == with_token
    assert not token.is_provisioned
    assert token.permissions.model_dump() == {
        "full_access": True,
        "read": ["bucket-1"],
        "write": ["bucket-2"],
    }
    assert token.ip_allowlist == []
    assert token.ttl is None
    assert token.is_expired is False
    assert token.last_access is None or isinstance(token.last_access, datetime)


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
    assert len(tokens) >= 2

    token_info = next((token for token in tokens if token.name == with_token), None)
    assert token_info is not None
    assert token_info.name == with_token
    assert token_info.created_at is not None
    assert token_info.last_access is None or isinstance(
        token_info.last_access, datetime
    )
    assert token_info.is_expired is False
    assert isinstance(token_info.ip_allowlist, list)


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__remove_token(client, with_token, random_prefix):
    """Should delete a token"""
    await client.remove_token(with_token)
    with pytest.raises(
        ReductError,
        match=f"Status 404: Token '{random_prefix}-test-token' doesn't exist",
    ):
        await client.get_token(with_token)


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__me(client):
    """Should get user info"""
    current_token: FullTokenInfo = await client.me()
    assert current_token.name == "init-token"
    assert current_token.permissions.model_dump() == {
        "full_access": True,
        "read": [],
        "write": [],
    }
    assert isinstance(current_token.ip_allowlist, list)
    assert current_token.last_access is None or isinstance(
        current_token.last_access, datetime
    )
    assert current_token.is_expired is False


@requires_env("RS_API_TOKEN")
@requires_api("1.19")
@pytest.mark.asyncio
async def test__create_token_with_ttl(client, random_prefix):
    """Should create a token with inactivity TTL"""
    name = f"{random_prefix}-ttl-token"
    try:
        _ = await client.create_token(
            name,
            Permissions(full_access=True),
            ttl=60,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            ip_allowlist=["203.0.113.10", "10.10.0.0/16"],
        )
    except ReductError as err:
        if err.status_code in (404, 405, 422):
            pytest.skip("Current ReductStore API doesn't support TTL token payload yet")
        raise

    token = await client.get_token(name)
    assert token.ttl == 60
    assert token.ip_allowlist == ["203.0.113.10", "10.10.0.0/16"]

    await client.remove_token(name)


@requires_env("RS_API_TOKEN")
@pytest.mark.asyncio
async def test__rotate_token(client, with_token, url):
    """Should rotate a token and invalidate previous token value"""
    old_value = await client.create_token(
        f"{with_token}-to-rotate",
        Permissions(full_access=True),
    )
    try:
        new_value = await client.rotate_token(f"{with_token}-to-rotate")
    except ReductError as err:
        await client.remove_token(f"{with_token}-to-rotate")
        if err.status_code in (404, 405):
            pytest.skip("Current ReductStore API doesn't support token rotation yet")
        raise

    assert new_value != old_value

    async with Client(url, api_token=old_value) as old_client:
        with pytest.raises(ReductError, match="Status 401"):
            await old_client.me()

    async with Client(url, api_token=new_value) as new_client:
        me = await new_client.me()
        assert me.name == f"{with_token}-to-rotate"

    await client.remove_token(f"{with_token}-to-rotate")


@pytest.mark.asyncio
async def test__with(url, api_token):
    """Should create a client with context manager"""
    async with Client(url, api_token=api_token) as client:
        bucket = await client.create_bucket("bucket-1", exist_ok=True)
        await bucket.info()
