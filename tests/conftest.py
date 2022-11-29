"""Common fixtures"""
import os

import pytest
import pytest_asyncio

from reduct import Client, Bucket


def requires_env(key):
    """Skip test if environment variable is not set"""
    env = os.environ.get(key)

    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test",
    )


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest_asyncio.fixture(name="client")
async def _make_client(url):
    api_token = os.getenv("RS_API_TOKEN", default=None)
    client = Client(url, api_token=api_token)
    buckets = await client.list()
    for info in buckets:
        bucket = await client.get_bucket(info.name)
        await bucket.remove()

    for token in await client.get_token_list():
        if token.name != "init-token":
            await client.remove_token(token.name)

    yield client


@pytest_asyncio.fixture(name="bucket_1")
async def _bucket_1(client) -> Bucket:
    bucket = await client.create_bucket("bucket-1")
    await bucket.write("entry-1", b"some-data-1", timestamp=1_000_000)
    await bucket.write("entry-1", b"some-data-2", timestamp=2_000_000)
    await bucket.write("entry-2", b"some-data-3", timestamp=3_000_000)
    await bucket.write("entry-2", b"some-data-4", timestamp=4_000_000)
    yield bucket
    await bucket.remove()


@pytest_asyncio.fixture(name="bucket_2")
async def _bucket_2(client) -> Bucket:
    bucket = await client.create_bucket("bucket-2")
    await bucket.write("entry-1", b"some-data-1", timestamp=5_000_000)
    await bucket.write("entry-1", b"some-data-2", timestamp=6_000_000)
    yield bucket
    await bucket.remove()
