# Subscribing to Data

This guide shows you how to use [ReductStore Python SDK](https://py.reduct.store/en/latest/) to subscribe to new
records in a bucket.

## Prerequisites

If you don't have a ReductStore instance running, you can easily spin one up as a Docker container. To do this, run the
following command:

```bash
docker run -p 8383:8383 reduct/store:latest
```

Also, if you haven't already installed the SDK, you can use the `pip` package manager to install the `reduct-py`
package:

```bash
pip install reduct-py
```

## Example

For demonstration purposes, we will create a script with two coroutines:

1. `writer` will write data to an entry in a bucket with the current timestamp and the label `good`.
2. `subscriber` will subscribe to the entry and records which have the label `good` equal to `True`.

When the `subscriber` will receive 10 records, it stops the subscription and the `writer`.

This is the whole script:

```python title="subscribing.py"
--8<-- "./examples/subscribing.py:"
```

The most important part of the script is the `subscriber` coroutine:

```python title="subscribing.py"
--8<-- "./examples/subscribing.py:subscriber"
```

The `subcribe` method queries records from the `entry-1` entry from the current time and subscribes to new records that have
the label `good` equal `true`. Since [ReductStore](https://www.reduct.store) provides an HTTP API, the `subscribe` method polls the entry for
each `poll_interval` seconds.

## When to use subscribing

The subscribing is useful when you want your application to be notified when new records are added to an entry. Some possible
use cases are:

* You can use ReductStore as a simple message broker with persistent storage.
* Replication of data from one ReductStore instance to another one. For example, your can subscribe to records with certain labels
  and write them to another ReductStore instance for long-term storage.
* Ingesting data from a ReductStore instance to a data warehouse.
