"""Record Tests"""

import pytest

from reduct.batch.batch_v1 import parse_batched_records_v1


class MockResponse:  # pylint: disable=too-few-public-methods
    """Mock response for testing"""

    class MockContent:  # pylint: disable=too-few-public-methods
        """Empty content iterator"""

        async def iter_chunked(self, _n):
            """Mock iter_chunked method"""
            yield b""

    def __init__(self):
        self.method = "GET"
        self.headers = {
            "X-Reduct-Time-1627849923": "0,text/plain,label1=value1,label2=value2",
            "X-Reduct-Time-1627849924": "0,application/json,labelA=valueA",
            "X-Reduct-Last": "true",
        }

        self.content = self.MockContent()

    def read(self, _size):
        """Mock read method"""
        return b""


@pytest.mark.asyncio
async def test_parsing_capitalized_headers():
    """Test parsing headers with capitalized field names"""

    resp = MockResponse()
    records = []
    async for record in parse_batched_records_v1(resp):
        records.append(record)

    assert len(records) == 2
    assert records[0].timestamp == 1627849923
    assert records[0].size == 0
    assert records[0].content_type == "text/plain"
    assert records[0].labels == {"label1": "value1", "label2": "value2"}
    assert records[1].timestamp == 1627849924
    assert records[1].size == 0
    assert records[1].content_type == "application/json"
    assert records[1].labels == {"labelA": "valueA"}
