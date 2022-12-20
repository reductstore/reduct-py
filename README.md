# ReductStore Client SDK for Python

[![PyPI](https://img.shields.io/pypi/v/reduct-py)](https://pypi.org/project/reduct-py/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/reduct-py)](https://pypi.org/project/reduct-py/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-py/ci.yml?branch=main)](https://github.com/reductstore/reduct-py/actions)

Asynchronous HTTP client for [ReductStore](https://www.reduct.store) written in Python.

## Features

* Support [ReductStore HTTP API v1.1](https://docs.reduct.store/http-api)
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
    client = Client('https://play.reduct.store')
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

* [Documentation](https://py.reduct.store/)
* [ReductStore HTTP API](https://docs.reduct.store/http-api)
