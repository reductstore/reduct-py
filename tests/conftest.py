import pytest
from reduct import Client, Bucket


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.fixture(name="client")
async def _make_client(url):
    client = Client(url)
    buckets = await client.list()
    for info in buckets:
        bucket = await client.get_bucket(info.name)
        await bucket.remove()

    yield client


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
