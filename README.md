# Reduct Storage Client SDK for Python

![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reduct-storage/reduct-py)
![PyPI - Downloads](https://img.shields.io/pypi/dm/reduct-py)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/reduct-storage/reduct-py/ci)

Asynchronous HTTP client for [Reduct Storage](https://reduct-storage.dev) written in Python.

## Features

* Support [Reduct Storage HTTP API v1.1](https://docs.reduct-storage.dev/http-api)
* Based on aiohttp and pydantic

## Install

```
pip install reduct-py
```

## Example

```python
import time
import asyncio
from reduct import Client, Bucket

async def main():
    client = Client('https://play.reduct-storage.dev')
    bucket: Bucket = await client.create_bucket("my-bucket", exist_ok=True)

    ts = time.time_ns() / 1000
    await bucket.write("entry-1", b"Hey!!", ts)
    async with bucket.read("entry-1", ts) as record:
        data = await record.read_all()
        print(data)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## References

* [Documentation](http://reduct-py.rtfd.io/)
* [Reduct Storage HTTP API](https://docs.reduct-storage.dev/http-api)
