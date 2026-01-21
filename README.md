# ReductStore Client SDK for Python

[![PyPI](https://img.shields.io/pypi/v/reduct-py)](https://pypi.org/project/reduct-py/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/reduct-py)](https://pypi.org/project/reduct-py/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-py/ci.yml?branch=main)](https://github.com/reductstore/reduct-py/actions)

This package provides an asynchronous HTTP client for interacting with  [ReductStore](https://www.reduct.store) in Python.

## Features

* Supports the [ReductStore HTTP API v1.18](https://www.reduct.store/docs/http-api)
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
from reduct import Client, BucketSettings, QuotaType


async def main():
    # 1. Create a ReductStore client
    async with Client("http://localhost:8383", api_token="my-token") as client:
        # 2. Get or create a bucket with 1Gb quota
        bucket = await client.create_bucket(
            "my-bucket",
            BucketSettings(quota_type=QuotaType.FIFO, quota_size=1_000_000_000),
            exist_ok=True,
        )

        # 3. Write some data with timestamps in the 'entry-1' entry
        await bucket.write("sensor-1", b"<Blob data>",
                           timestamp="2024-01-01T10:00:00Z",
                           labels={"score": 10})
        await bucket.write("sensor-2", b"<Blob data>",
                           timestamp="2024-01-01T10:00:01Z",
                           labels={"score": 20})

        # 4. Query the data by time range and condition
        async for record in bucket.query("sensor-*",
                                         start="2024-01-01T10:00:00Z",
                                         stop="2024-01-01T10:00:02Z",
                                         when={"&score": {"$gt": 10}}):
            print(f"Record entry: {record.entry}")
            print(f"Record timestamp: {record.timestamp}")
            print(f"Record size: {record.size}")
            print(await record.read_all())


# 5. Run the main function
if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

```

For more examples, see the [Guides](https://reduct.store/docs/guides) section in the ReductStore documentation.


### Supported ReductStore Versions and Backward Compatibility

The library is backward compatible with the previous versions. However, some methods have been deprecated and will be
removed in the future releases. Please refer to **CHANGELOG.md** for more details.
The SDK supports the following ReductStore API versions:

* v1.18
* v1.17
* v1.16

It can work with newer and older versions, but it is not guaranteed that all features will work as expected because
the API may change and some features may be deprecated or the SDK may not support them yet.
