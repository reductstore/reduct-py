# Quick Start

This quick start guide will walk you through the process of installing and using the ReductStore Python client SDK to
interact with a [ReductStore](https://github.com/reductstore/reductstore) instance.

## Installing the SDK

To install the ReductStore SDK, you will need to have Python 3.7 or higher installed on your machine. Once Python is
installed, you can use the `pip` package manager to install the `reduct-py` package:

```
pip install reduct-py
```

## Running ReductStore as a Docker Container

If you don't already have a ReductStore instance running, you can easily spin one up as a Docker container. To do so,
run the following command:

```
docker run -p 8383:8383 reduct/store:latest
```

This will start a ReductStore instance listening on port 8383 of your local machine.

## Using the SDK

Now that you have the `reduct-py` SDK installed and a ReductStore instance running, you can start using the SDK to
interact with the ReductStore service.

Here is an example of using the Python SDK to perform a few different operations on a bucket:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:"
```

Let's break down what this example is doing.

### Creating a Client

To create a ReductStore client, you can use the `Client` class from the `reduct` module. Pass the URL of the ReductStore
instance you want to connect to as an argument to the `Client` constructor. To reuse the same HTTP session and improve
performance, you can use the context manager:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:createclient"
```

### Creating a Bucket

To get or create a bucket, you can use the `create_bucket` method on a `Client` instance. Pass the name of the bucket
you
want to get or create as an argument, along with a `BucketSettings` object to specify the desired quota for the bucket.
Set the `exist_ok` argument to True to create the bucket if it does not exist:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:createbucket"
```

### Writing data to a Bucket

To write data to an entry in a bucket with the current timestamp, you can use the `write` method on a `Bucket` instance.
Pass the name of the entry you want to write to as an argument, along with the data you want to write. The `write`
method
will automatically use the current timestamp when writing the data to the entry:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:writedata"
```

This is the simplest case for writing data to an entry in a bucket using the SDK. The write method also allows
you to specify a custom timestamp. Let's look at an example of uploading data in chunks with a custom timestamp:

To upload a file in chunks to an entry in a bucket, pass the name of
the entry you want to write to as an argument, along with a generator function that yields the file data in chunks.
Specify the total size of the file with the `content_length` argument, and specify a custom timestamp with
the `timestamp`
argument:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:uploadfile"
```

This code snippet shows how to use the write method to upload a file in chunks to an entry in a bucket. The generator
function `file_reader` reads the file in chunks of 50 bytes and yields the data. The write method reads the data from
the
generator and writes it to the entry in the bucket.

### Reading data from a Bucket

To read data from an entry in a bucket, you can use the `read` method on a `Bucket` instance. Pass the name of the entry
you
want to read from and the timestamp of the specific record you want to read as arguments. Use the async with statement
to open the record, and then use the `read_all` method to read all of the data in the record:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:readdata"
```

To iterate over all records in an entry, you can use the `query` method on a Bucket instance. Pass the name of the entry
you want to iterate over as an argument. Use a for loop to iterate over the records, and use the read method on each
record to read the data in chunks:

```python title="quick_start.py"
--8 < -- "./examples/quick_start.py:iteraterecords"
```

The `query` method returns an async iterator to the records in the entry. By default, the method returns all records
in the entry. However, you can use the `start` and `stop` arguments to specify a time interval for the records you want
to
retrieve. The `start` argument specifies the beginning of the time interval, and the `stop` argument specifies the end
of
the time interval. Both arguments are timestamps in UNIX microseconds.

```python
start_ts = 1609459200000000  # January 1, 2021 at 00:00:00
stop_ts = 1609827200000000  # January 31, 2021 at 23:59:59
async for record in bucket.query("entry-1", start=start_ts, stop=stop_ts):
# Process the record
```

## Next Steps

You can find more detailed documentation and examples in [the Reference API section](./api/bucket.md). You can also
refer to the [ReductStore HTTP API](https://docs.reduct.store/http-api) documentation for a complete reference
of the available API calls.
