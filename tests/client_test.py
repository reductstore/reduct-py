"""Just stub for pipline"""
import pytest
from reduct import Client, ServerInfo


@pytest.fixture(name="url")
def _url() -> str:
    return "http://127.0.0.1:8383"


@pytest.mark.asyncio
async def test__info(url):
    """Should get information about storage"""
    client = Client(url)
    info: ServerInfo = await client.info()

    assert info.version >= "0.4.0"
