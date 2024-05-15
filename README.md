# ReductStore Client SDK for Python

[![PyPI](https://img.shields.io/pypi/v/reduct-py)](https://pypi.org/project/reduct-py/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/reduct-py)](https://pypi.org/project/reduct-py/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-py/ci.yml?branch=main)](https://github.com/reductstore/reduct-py/actions)

This package provides an asynchronous HTTP client for interacting with  [ReductStore](https://www.reduct.store) in Python.

## Features

* Supports the [ReductStore HTTP API v1.10](https://reduct.store/docs/http-api)
* Bucket management
* API Token management
* Write, read and query data
* Labeling records
* Batching records for read and write operations
* Subscription
* Replication management

## Install

To install this package, run the following command:

```
pip install reduct-py
```

## Example

Here is an example of how to use this package to create a bucket, write data to it, and read data from it:

```python
from datetime import datetime
import asyncio
from reduct import Client, Bucket

async def main():
    # Create a client for interacting with a ReductStore service
    client = Client("http://localhost:8383")

    # Create a bucket and store a reference to it in the `bucket` variable
    bucket: Bucket = await client.create_bucket("my-bucket", exist_ok=True)

    # Write data to the bucket
    ts = datetime.now()
    await bucket.write("entry-1", b"Hey!!", ts)

    # Read data from the bucket
    async with bucket.read("entry-1", ts) as record:
        data = await record.read_all()
        print(data)

# Run the main function
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

For more examples, see the [Quick Start](https://py.reduct.store/en/latest/docs/quick-start/).

## References

* [Documentation](https://py.reduct.store/)
* [ReductStore HTTP API](https://reduct.store/docs/http-api)
