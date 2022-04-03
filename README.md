# Reduct Storage Client SDK for Python

Asynchronous HTTP client for [Reduct Storage](https://reduct-storage.dev) written in Python.

## Features

* Support Reduct Storage HTTP API v0.4 (in progress)
* Based on aiohttp

## Example

```python
import asyncio
from reduct import Client, Bucket

client = Client('http://127.0.0.1:8383')

async def read_data():
    bucket: Bucket = await client.get_bucket('my-bucket')
    msg = await bucket.read('entry-1')  # read the last record in record
    print(msg)

async def write_data(msg: bytes):
    bucket: Bucket = await client.get_bucket('my-bucket')
    await bucket.write('entry-1', msg)

loop = asyncio.get_event_loop()
loop.run_until_complete(write_data(b"hello!"))
loop.run_until_complete(read_data())
```

## References

* [Documentation](https://reduct-py.rthd.io)
* [Reduct Storage HTTP API](https://docs.reduct-storage.dev/http-api)
