"""Tests for Bucket"""

import asyncio
import time
from datetime import datetime

from typing import List, Tuple

import pytest

from reduct import ReductError, BucketSettings, QuotaType, Record, BucketFullInfo
from reduct.record import Batch
from tests.conftest import requires_api


@pytest.mark.asyncio
async def test__remove_ok(client):
    """Should remove a bucket"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await client.get_bucket("bucket")


@pytest.mark.asyncio
async def test__remove_not_exist(client):
    """Should not remove a bucket if it doesn't exist"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await bucket.remove()


@pytest.mark.asyncio
@requires_api("1.6")
async def test__remove_entry(bucket_1):
    """Should remove an entry in a bucket"""
    await bucket_1.remove_entry("entry-2")
    assert "entry-2" not in [entry.name for entry in await bucket_1.get_entry_list()]


@pytest.mark.asyncio
async def test__set_settings(bucket_1):
    """Should set new settings"""
    await bucket_1.set_settings(BucketSettings(max_block_records=10000))
    new_settings = await bucket_1.get_settings()
    assert new_settings.model_dump() == {
        "max_block_size": 64000000,
        "max_block_records": 10000,
        "quota_size": 0,
        "quota_type": QuotaType.NONE,
    }


@pytest.mark.asyncio
async def test__get_info(bucket_2):
    """Should get info about bucket"""
    info = await bucket_2.info()
    assert info.model_dump() == {
        "entry_count": 1,
        "latest_record": 6000000,
        "name": "bucket-2",
        "oldest_record": 5000000,
        "size": 108,
        "is_provisioned": False,
    }


@pytest.mark.asyncio
async def test__get_full_info(bucket_2):
    """Should get full info about bucket"""
    info: BucketFullInfo = await bucket_2.get_full_info()
    assert info.info == await bucket_2.info()
    assert info.settings == await bucket_2.get_settings()
    assert info.entries == await bucket_2.get_entry_list()


@pytest.mark.asyncio
async def test__get_entries(bucket_1):
    """Should get list of entries"""
    entries = await bucket_1.get_entry_list()
    assert len(entries) == 2
    assert entries[0].model_dump() == {
        "block_count": 1,
        "latest_record": 2000000,
        "name": "entry-1",
        "oldest_record": 1000000,
        "record_count": 2,
        "size": 108,
    }

    assert entries[1].model_dump() == {
        "block_count": 1,
        "latest_record": 5000000,
        "name": "entry-2",
        "oldest_record": 3000000,
        "record_count": 3,
        "size": 157,
    }


@pytest.mark.parametrize("head, content", [(True, b""), (False, b"some-data-3")])
@pytest.mark.parametrize(
    "timestamp",
    [3_000_000, datetime.fromtimestamp(3), 3.0, datetime.fromtimestamp(3).isoformat()],
)
@pytest.mark.asyncio
async def test__read_by_timestamp(bucket_1, head, content, timestamp):
    """Should read a record by timestamp"""
    async with bucket_1.read("entry-2", timestamp=timestamp, head=head) as record:
        data = await record.read_all()
        assert data == content
        assert record.timestamp == 3_000_000
        assert record.size == 11
        assert record.content_type == "application/octet-stream"


@pytest.mark.asyncio
async def test__read_latest(bucket_1):
    """Should read the latest record if no timestamp"""
    async with bucket_1.read("entry-2") as record:
        data = await record.read_all()
        assert data == b"some-data-5"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "timestamp",
    [5_000_000, datetime.now(), "2021-01-01T00:00:00Z", datetime.now().timestamp()],
)
async def test__write_by_timestamp(bucket_2, timestamp):
    """Should write a record by timestamp"""
    await bucket_2.write("entry-3", b"test-data", timestamp=timestamp)
    async with bucket_2.read("entry-3", timestamp=timestamp) as record:
        data = await record.read_all()
        assert data == b"test-data"


@pytest.mark.asyncio
async def test__write_with_current_time(bucket_2):
    """Should write a record with current time"""
    belated_timestamp = int(time.time_ns() / 1000)
    await asyncio.sleep(0.01)

    await bucket_2.write("entry-3", b"test-data")
    await bucket_2.write("entry-3", b"old-data", timestamp=belated_timestamp)
    async with bucket_2.read("entry-3") as record:
        data = await record.read_all()
        assert data == b"test-data"


@pytest.mark.asyncio
async def test__write_in_chunks(bucket_2):
    """Should accept interator for writing in chunks"""

    async def sender():
        for chunk in [b"part1", b"part2"]:
            yield chunk

    await bucket_2.write("entry-1", sender(), content_length=10)
    async with bucket_2.read("entry-1") as record:
        data = await record.read_all()
        assert data == b"part1part2"


@pytest.mark.asyncio
async def test__write_with_labels(bucket_1):
    """Should write data with labels"""
    await bucket_1.write(
        "entry-1", b"something", labels={"label1": 123, "label2": 0.1, "label3": "hey"}
    )
    async with bucket_1.read("entry-1") as record:
        data = await record.read_all()
        assert data == b"something"
        assert record.labels == {"label1": "123", "label2": "0.1", "label3": "hey"}


@pytest.mark.asyncio
async def test__write_with_content_type(bucket_1):
    """Should write data with content_type"""
    await bucket_1.write("entry-1", b"something", content_type="text/plain")

    async with bucket_1.read("entry-1") as record:
        data = await record.read_all()
        assert data == b"something"
        assert record.content_type == "text/plain"


@pytest.mark.asyncio
async def test_write_big_blob(bucket_1):
    """Should write big blob and stop upload if http status is not 200"""
    await bucket_1.write("entry-1", b"1" * 1000000, timestamp=1)
    with pytest.raises(ReductError, match="409"):
        await bucket_1.write("entry-1", b"1" * 10_000_000, timestamp=1)


@pytest.mark.parametrize(
    "head, content", [(True, [b"", b""]), (False, [b"some-data-3", b"some-data-4"])]
)
@pytest.mark.parametrize(
    "start, stop",
    [
        (0, 5_000_000),
        (datetime.fromtimestamp(0), datetime.fromtimestamp(5)),
        (datetime.fromtimestamp(0).isoformat(), datetime.fromtimestamp(5).isoformat()),
        (0, 5.0),
    ],
)
@pytest.mark.asyncio
async def test_query_records(bucket_1, head, content, start, stop):
    """Should query records for a time interval"""
    records: List[Tuple[Record, bytes]] = [
        (record, await record.read_all())
        async for record in bucket_1.query(
            "entry-2", start=start, stop=stop, ttl=5, head=head
        )
    ]
    assert len(records) == 2

    assert records[0][0].timestamp == 3000000
    assert records[0][0].size == 11
    assert records[0][0].content_type == "application/octet-stream"
    assert records[0][1] == content[0]

    assert records[1][0].timestamp == 4000000
    assert records[1][0].size == 11
    assert records[1][0].content_type == "application/octet-stream"
    assert records[1][1] == content[1]


@pytest.mark.asyncio
async def test_query_records_first(bucket_1):
    """Should query records for from first record"""

    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", stop=4_000_000)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 3_000_000


@pytest.mark.asyncio
async def test_query_records_included_labels(bucket_1):
    """Should query records including certain labels"""
    await bucket_1.write(
        "entry-1", b"data1", labels={"label1": "value1", "label2": "value2"}
    )
    await bucket_1.write(
        "entry-1", b"data2", labels={"label1": "value1", "label2": "value3"}
    )

    records: List[Record] = [
        record
        async for record in bucket_1.query(
            "entry-1", include={"label1": "value1", "label2": "value2"}
        )
    ]

    assert len(records) == 1
    assert records[0].labels == {"label1": "value1", "label2": "value2"}


@pytest.mark.asyncio
async def test_query_records_excluded_labels(bucket_2):
    """Should query records excluding certain labels"""
    await bucket_2.write(
        "entry-3", b"data1", labels={"label1": "value1", "label2": "value2"}
    )
    await bucket_2.write(
        "entry-3", b"data2", labels={"label1": "value1", "label2": "value3"}
    )
    records: List[Record] = [
        record
        async for record in bucket_2.query(
            "entry-3", exclude={"label1": "value1", "label2": "value2"}
        )
    ]

    assert len(records) == 1
    assert records[0].labels == {"label1": "value1", "label2": "value3"}


@pytest.mark.asyncio
async def test_query_records_last(bucket_1):
    """Should query records for until last record"""
    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", start=5_000_000)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 5_000_000


@pytest.mark.asyncio
@requires_api("1.6")
async def test_query_records_limit(bucket_1):
    """Should query records for until last record"""
    records: List[Record] = [
        record async for record in bucket_1.query("entry-1", start=0, limit=1)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 1000000


@pytest.mark.asyncio
async def test_query_records_all(bucket_1):
    """Should query records all data"""
    records = [record async for record in bucket_1.query("entry-2")]
    assert len(records) == 3


@pytest.mark.asyncio
async def test_read_record_in_chunks(bucket_1):
    """Should provide records with read method and read in chunks"""
    data = [await record.read_all() async for record in bucket_1.query("entry-2")]
    assert data == [b"some-data-3", b"some-data-4", b"some-data-5"]

    data = []

    async for record in bucket_1.query("entry-2"):
        async for chunk in record.read(n=4):
            data.append(chunk)

    assert data == [
        b"some",
        b"-dat",
        b"a-3",
        b"some",
        b"-dat",
        b"a-4",
        b"some",
        b"-dat",
        b"a-5",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 100, 10_000, 1_000_000])
async def test_read_record_in_chunks_full(bucket_1, size):
    """Should provide records with read method and read with max size"""
    blob = b"1" * size
    await bucket_1.write("entry-5", blob, timestamp=1)

    data = b""
    async for record in bucket_1.query("entry-5"):
        async for chunk in record.read(n=record.size):
            data += chunk

    assert data == blob


@pytest.mark.asyncio
async def test_no_content_query(bucket_1):
    """Should return empty list if no content"""
    records = [
        record
        async for record in bucket_1.query("entry-2", include={"label1": "value1"})
    ]
    assert len(records) == 0


@pytest.mark.asyncio
async def test_subscribe(bucket_1):
    """Should subscribe to new records"""
    data = []

    async def subscriber():
        async for record in bucket_1.subscribe("entry-2"):
            data.append(await record.read_all())
            if record.labels.get("stop", "") == "true":
                break

    await asyncio.gather(
        subscriber(), bucket_1.write("entry-2", b"some-data-6", labels={"stop": "true"})
    )

    assert data == [b"some-data-3", b"some-data-4", b"some-data-5", b"some-data-6"]


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 100, 10_000, 1_000_000])
async def test_read_batched_records_in_random_order(bucket_1, size):
    """Should read batched records in random order (read_all)"""

    await bucket_1.write("entry-3", b"1" * size, timestamp=1)
    await bucket_1.write("entry-3", b"2" * size, timestamp=2)
    await bucket_1.write("entry-3", b"3" * size, timestamp=3)

    records = []
    async for record in bucket_1.query("entry-3"):
        records.append(record)

        if len(records) == 3:
            assert records[0].timestamp == 1
            assert records[1].timestamp == 2
            assert records[2].timestamp == 3

            assert (await records[1].read_all()) == (b"2" * size)
            assert (await records[0].read_all()) == (b"1" * size)
            assert (await records[2].read_all()) == (b"3" * size)


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 100, 10_000, 1_000_000])
async def test_read_batched_records_in_random_order_chunks(bucket_1, size):
    """Should read batched records in random order (read in chunks)"""

    await bucket_1.write("entry-3", b"1" * size, timestamp=1)
    await bucket_1.write("entry-3", b"2" * size, timestamp=2)
    await bucket_1.write("entry-3", b"3" * size, timestamp=3)

    async def read_chunks(rec: Record):
        buffer = b""
        async for chunk in rec.read(1024):
            buffer += chunk
        return buffer

    records = []
    async for record in bucket_1.query("entry-3"):
        records.append(record)

        if len(records) == 3:
            data = await read_chunks(records[1])
            assert data == (b"2" * size)

            data = await read_chunks(records[0])
            assert data == (b"1" * size)

            data = await read_chunks(records[2])
            assert data == (b"3" * size)


@requires_api("1.7")
@pytest.mark.asyncio
async def test_batched_write(bucket_1):
    """Should write batched records"""
    batch = Batch()
    # use different timestamp formats
    batch.add(1000, b"Hey,", "plain/text", {"label1": "value1"})
    batch.add(datetime.fromtimestamp(0.002), b"how", "plain/text", {"label2": "value2"})
    batch.add(
        datetime.fromtimestamp(0.003).isoformat(),
        b"are",
        "plain/text",
    )
    batch.add(4000, b"you?")

    await bucket_1.write_batch("entry-3", batch)

    records = [record async for record in bucket_1.query("entry-3")]
    frase = b" ".join(
        [await record.read_all() async for record in bucket_1.query("entry-3")]
    )
    assert len(records) == 4

    assert records[0].timestamp == 1000
    assert records[0].content_type == "plain/text"
    assert records[0].labels == {"label1": "value1"}

    assert records[1].timestamp == 2000
    assert records[1].content_type == "plain/text"
    assert records[1].labels == {"label2": "value2"}

    assert records[2].timestamp == 3000
    assert records[2].content_type == "plain/text"
    assert records[2].labels == {}

    assert records[3].timestamp == 4000
    assert records[3].content_type == "application/octet-stream"
    assert records[3].labels == {}

    assert frase == b"Hey, how are you?"


@requires_api("1.7")
@pytest.mark.asyncio
async def test_batched_write_with_errors(bucket_1):
    """Should write batched records and return errors"""

    await bucket_1.write("entry-3", b"1", timestamp=1)

    batch = Batch()
    batch.add(1, b"new")
    batch.add(2, b"reocrd")

    errors = await bucket_1.write_batch("entry-3", batch)
    assert len(errors) == 1
    assert errors[1] == ReductError(409, "A record with timestamp 1 already exists")


@pytest.mark.asyncio
@requires_api("1.10")
async def test_query_records_each_s(bucket_1):
    """Should query a record per 2 seconds"""
    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", start=0, each_s=2.0)
    ]
    assert len(records) == 2
    assert records[0].timestamp == 3000000
    assert records[1].timestamp == 5000000


@pytest.mark.asyncio
@requires_api("1.10")
async def test_query_records_each_n(bucket_1):
    """Should query each 3d records"""
    records: List[Record] = [
        record async for record in bucket_1.query("entry-2", start=0, each_n=3)
    ]
    assert len(records) == 1
    assert records[0].timestamp == 3000000


@pytest.mark.asyncio
@requires_api("1.11")
async def test_update_labels(bucket_1):
    """Should update labels of a record"""
    await bucket_1.update(
        "entry-2", 3000000, {"label1": "new-value", "label2": "", "label3": "value3"}
    )

    async with bucket_1.read("entry-2", timestamp=3000000) as record:
        assert record.labels == {"label1": "new-value", "label3": "value3"}


@pytest.mark.asyncio
@requires_api("1.11")
async def test_update_labels_batch(bucket_1):
    """Should update labels of records in a batch"""
    batch = Batch()
    batch.add(3000000, labels={"label1": "new-value", "label2": "", "label3": "value3"})
    batch.add(4000000, labels={"label1": "new-value", "label2": "", "label4": "value4"})
    batch.add(8000000)

    errors = await bucket_1.update_batch("entry-2", batch)
    assert len(errors) == 1
    assert errors[8000000] == ReductError(404, "No record with timestamp 8000000")

    async with bucket_1.read("entry-2", timestamp=3000000) as record:
        assert record.labels == {"label1": "new-value", "label3": "value3"}

    async with bucket_1.read("entry-2", timestamp=4000000) as record:
        assert record.labels == {"label1": "new-value", "label4": "value4"}
