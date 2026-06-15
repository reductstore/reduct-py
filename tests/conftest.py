"""Common fixtures"""

import os
import random
import string
from typing import Optional, Any, AsyncGenerator

import pytest
import pytest_asyncio
import requests

from reduct import (
    Client,
    Bucket,
    ReductError,
    ReplicationSettings,
    LifecycleSettings,
    LifecycleType,
)
from reduct.http import _extract_api_version


def _get_api_version() -> str:
    return requests.get("http://127.0.0.1:8383/api/v1/info", timeout=1.0).headers[
        "x-reduct-api"
    ]


def _supports_api(version: str) -> bool:
    return (
        _extract_api_version(version)[1] <= _extract_api_version(_get_api_version())[1]
    )


async def _remove_bucket_if_possible(bucket: Bucket) -> None:
    try:
        await bucket.remove()
    except ReductError as err:
        # 404: already removed, 409: async deletion still in progress.
        if err.status_code in (404, 409):
            return
        raise


def requires_env(key):
    """Skip test if environment variable is not set"""
    env = os.environ.get(key)

    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test",
    )


def requires_api(version):
    """Skip test if API version is not supported"""
    current_version = _get_api_version()
    return pytest.mark.skipif(
        not _supports_api(version),
        reason=f"Not suitable API version {current_version} for current test",
    )


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.fixture(name="api_token")
def _token() -> Optional[str]:
    api_token = os.getenv("RS_API_TOKEN", default=None)
    return api_token


@pytest.fixture(name="random_prefix")
def _prefix() -> str:
    prefix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return prefix


@pytest_asyncio.fixture(name="client")
async def _make_client(url, api_token, random_prefix):
    client = Client(url, api_token=api_token)
    buckets = await client.list()
    for info in buckets:
        if info.name.startswith(random_prefix):
            bucket = await client.get_bucket(info.name)
            await _remove_bucket_if_possible(bucket)

    for token in await client.get_token_list():
        if token.name != "init-token" and token.name.startswith(random_prefix):
            await client.remove_token(token.name)

    for replication in await client.get_replications():
        if replication.name.startswith(random_prefix):
            await client.delete_replication(replication.name)

    if _supports_api("1.20"):
        for lifecycle in await client.get_lifecycles():
            if lifecycle.name.startswith(random_prefix):
                await client.delete_lifecycle(lifecycle.name)

    yield client


@pytest_asyncio.fixture(name="bucket_1")
async def _bucket_1(client, random_prefix) -> AsyncGenerator[Bucket, Any]:
    bucket = await client.create_bucket(f"{random_prefix}-bucket-1")
    await bucket.write(
        "entry-1", b"some-data-1", timestamp=1_000_000, labels={"number": 1}
    )
    await bucket.write(
        "entry-1", b"some-data-2", timestamp=2_000_000, labels={"number": 2}
    )
    await bucket.write(
        "entry-2", b"some-data-3", timestamp=3_000_000, labels={"number": 1}
    )
    await bucket.write(
        "entry-2", b"some-data-4", timestamp=4_000_000, labels={"number": 2}
    )
    await bucket.write(
        "entry-2", b"some-data-5", timestamp=5_000_000, labels={"number": 3}
    )

    yield bucket
    await _remove_bucket_if_possible(bucket)


@pytest_asyncio.fixture(name="bucket_2")
async def _bucket_2(client, random_prefix) -> AsyncGenerator[Bucket, Any]:
    bucket = await client.create_bucket(f"{random_prefix}-bucket-2")
    await bucket.write("entry-1", b"some-data-1", timestamp=5_000_000)
    await bucket.write("entry-1", b"some-data-2", timestamp=6_000_000)
    yield bucket
    await _remove_bucket_if_possible(bucket)


@pytest_asyncio.fixture(name="replication_1")
async def _replication_1(
    client, bucket_1, bucket_2, random_prefix
) -> AsyncGenerator[str, Any]:
    replication_name = f"{random_prefix}-replication-1"
    replication_settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="http://127.0.0.1:8383",
    )
    await client.create_replication(replication_name, replication_settings)
    yield replication_name
    try:
        await client.delete_replication(replication_name)
    except ReductError as err:
        if err.status_code != 404:
            raise


@pytest_asyncio.fixture(name="replication_2")
async def _replication_2(
    client, bucket_1, bucket_2, random_prefix
) -> AsyncGenerator[str, Any]:
    replication_name = f"{random_prefix}-replication-2"
    replication_settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="http://127.0.0.1:8383",
    )
    await client.create_replication(replication_name, replication_settings)
    yield replication_name
    try:
        await client.delete_replication(replication_name)
    except ReductError as err:
        if err.status_code != 404:
            raise


@pytest_asyncio.fixture(name="temporary_replication")
async def _temporary_replication(
    client, bucket_1, bucket_2, random_prefix
) -> AsyncGenerator[str, Any]:
    replication_name = f"{random_prefix}-temp-replication"
    replication_settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="http://127.0.0.1:8383",
    )
    await client.create_replication(replication_name, replication_settings)
    yield replication_name
    try:
        await client.delete_replication(replication_name)
    except ReductError as err:
        if err.status_code != 404:
            raise


@pytest_asyncio.fixture(name="lifecycle_1")
async def _lifecycle_1(client, bucket_1, random_prefix) -> AsyncGenerator[str, Any]:
    lifecycle_name = f"{random_prefix}-lifecycle-1"
    lifecycle_settings = LifecycleSettings(
        bucket=bucket_1.name,
        older_than="1h",
        interval="10m",
    )
    await client.create_lifecycle(lifecycle_name, lifecycle_settings)
    yield lifecycle_name
    await client.delete_lifecycle(lifecycle_name)


@pytest_asyncio.fixture(name="lifecycle_2")
async def _lifecycle_2(client, bucket_1, random_prefix) -> AsyncGenerator[str, Any]:
    lifecycle_name = f"{random_prefix}-lifecycle-2"
    lifecycle_settings = LifecycleSettings(
        bucket=bucket_1.name,
        older_than="2h",
        interval="20m",
        type=LifecycleType.COMPRESS,
    )
    await client.create_lifecycle(lifecycle_name, lifecycle_settings)
    yield lifecycle_name
    await client.delete_lifecycle(lifecycle_name)


@pytest_asyncio.fixture(name="temporary_lifecycle")
async def _temporary_lifecycle(
    client, bucket_1, random_prefix
) -> AsyncGenerator[str, Any]:
    lifecycle_name = f"{random_prefix}-temp-lifecycle"
    lifecycle_settings = LifecycleSettings(
        bucket=bucket_1.name,
        older_than="1h",
        interval="10m",
    )
    await client.create_lifecycle(lifecycle_name, lifecycle_settings)
    yield lifecycle_name
    try:
        await client.delete_lifecycle(lifecycle_name)
    except ReductError as err:
        if err.status_code != 404:
            raise
