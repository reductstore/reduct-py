import asyncio
from time import time_ns

from reduct import Client, Bucket

client = Client("http://127.0.0.1:8383")
running = True


async def writer():
    """Write a blob with toggling good flag"""
    bucket: Bucket = await client.create_bucket("bucket", exist_ok=True)
    good = True
    while running:
        data = b"Some blob of data"
        ts = int(time_ns() / 10000)
        await bucket.write("entry-1", data, ts, labels=dict(good=good))
        print(f"Record written: ts={ts}, good={good}")
        good = not good
        await asyncio.sleep(1)


# --8<-- [start:subscriber]
async def subscriber():
    """Subscribe on good records and exit after ten received"""
    global running
    bucket: Bucket = await client.create_bucket("bucket", exist_ok=True)
    counter = 0
    async for record in bucket.subscribe(
        "entry-1",
        start=int(time_ns() / 10000),
        poll_interval=1,
        include=dict(good=True),
    ):
        print(f"Good record received: ts={record.timestamp}, labels={record.labels}")
        counter += 1
        if counter >= 10:
            running = False
            break


# --8<-- [end:subscriber]

if __name__ == "__main__":
    loop = asyncio.new_event_loop()

    loop.create_task(writer())
    loop.create_task(subscriber())
    while running:
        loop.run_until_complete(asyncio.sleep(1))
