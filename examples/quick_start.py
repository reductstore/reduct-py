from pathlib import Path
from time import time_ns

from reduct import Client, BucketSettings, QuotaType

CURRENT_FILE = Path(__file__)


async def main():
    # --8<-- [start:createclient]
    # Create a ReductStore client
    client = Client("http://localhost:8383")
    # --8<-- [end:createclient]

    # --8<-- [start:createbucket]
    # Get or create a bucket with 1Gb quota
    bucket = await client.create_bucket(
        "my-bucket",
        BucketSettings(quota_type=QuotaType.FIFO, quota_size=1_000_000_000),
        exist_ok=True,
    )
    # --8<-- [end:createbucket]

    # --8<-- [start:writedata]
    # The simplest case. Write some data with the current timestamp
    await bucket.write("entry-1", b"Hello, World!")
    # --8<-- [end:writedata]

    # --8<-- [start:uploadfile]
    # More complex case. Upload a file in chunks with a custom timestamp unix timestamp in milliseconds
    async def file_reader():
        """Read the current example in chunks of 50 bytes"""
        with open(CURRENT_FILE, "rb") as file:
            while True:
                data = file.read(50)  # Read in chunks of 50 bytes
                if not data:
                    break
                yield data

    ts = int(time_ns() / 10000)
    await bucket.write(
        "entry-1",
        file_reader(),
        timestamp=ts,
        content_length=CURRENT_FILE.stat().st_size,
    )
    # --8<-- [end:uploadfile]

    # --8<-- [start:readdata]
    # The simplest case. Read the data by a certain ts
    async with bucket.read("entry-1", timestamp=ts) as record:
        print(f"Record timestamp: {record.timestamp}")
        print(f"Record size: {record.size}")
        print(await record.read_all())
    # --8<-- [end:readdata]

    # --8<-- [start:iteraterecords]
    # More complex case. Iterate over all records in the entry and read them in chunks
    async for record in bucket.query("entry-1"):
        print(f"Record timestamp: {record.timestamp}")
        print(f"Record size: {record.size}")
        async for chunk in record.read(50):
            print(chunk)
    # --8<-- [end:iteraterecords]


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
