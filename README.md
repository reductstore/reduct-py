# Reduct Storage Client SDK for Python

Asynchronous HTTP client for [Reduct Storage](https://reduct-storage.dev) written in Python.

## Features

* Support Reduct Storage HTTP API v0.4 (in progress)
* Based on aiohttp

## Example

```python
import time
import asyncio
from reduct import Client, Bucket

async def main():
    client = Client('https://play.reduct-storage.dev')
    bucket: Bucket = await client.create_bucket("my-bucket")

    ts = time.time()
    await bucket.write("entry-1", b"Hey!!", ts)
    data = await bucket.read("entry-1", ts)
    print(data)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## References

* [Documentation](https://reduct-py.rthd.io)
* [Reduct Storage HTTP API](https://docs.reduct-storage.dev/http-api)
