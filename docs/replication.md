# Replicate Data Between Buckets

This guide explains how to use the ReductStore Python SDK to work with replications. Replications allow you to synchronize data between two buckets. 

The destination bucket can be on the same ReductStore instance or on a different instance (e.g., a ReductStore instance running on a remote server).

## Prerequisites

Before you start working with replications, ensure that you have the following:

1. A running ReductStore instance with version 1.8.0 or higher. You can download the latest version of ReductStore from [here](https://reduct.store/download).
2. A source bucket and a destination bucket. See the [Quick Start](./quick-start.md) guide for instructions on how to create buckets.
4. An instance of the ReductStore Python SDK client. See the [Quick Start](./quick-start.md) guide for instructions on how to create a client instance.

## Replication Concepts

### What is Replication?

Replication is a feature that enables you to create and manage data synchronization tasks between two buckets. It allows you to mirror data from a source bucket to a destination bucket.

### Key Components

To understand how replication works, let's look at its key components:

- **Source Bucket:** The bucket from which data is replicated.
- **Destination Bucket:** The bucket to which data is copied.
- **Destination Host:** The URL of the destination host (e.g., https://play.reduct.store).
- **Replication Name:** A unique name for the replication task.
- **Replication Settings:** Configuration options for the replication, such as specifying which records to replicate and which to exclude.

## Working with Replications

The ReductStore Python SDK provides several methods to work with replications. Here's an overview of these methods:

### 1. Create a New Replication

Create a new replication with the specified settings.

```python
from reduct import ReplicationSettings

replication_settings = ReplicationSettings(
    src_bucket="source-bucket",
    dst_bucket="destination-bucket",
    dst_host="https://play.reduct.store",
)
await client.create_replication("my-replication", replication_settings)
```

### 2. Get a List of Replications

You can retrieve a list of all replications, along with their statuses.

```python
replications = await client.get_replications()
```

### 3. Get Detailed Information about a Replication

Get detailed information about a specific replication using its name.

```python
replication_detail = await client.get_replication_detail("my-replication")
```

### 4. Update an Existing Replication

Update the settings of an existing replication.

```python
from reduct import ReplicationSettings

new_settings = ReplicationSettings(
    src_bucket="updated-source-bucket",
    dst_bucket="updated-destination-bucket",
    dst_host="https://play.reduct.store",
)
await client.update_replication("my-replication", new_settings)
```

### 5. Delete a Replication

Delete a replication by specifying its name.

```python
await client.delete_replication("my-replication")
```

## Next Steps

Refer to the [Reference API Section](./api/client.md) for detailed documentation about the Python Client. For a comprehensive reference of all API calls, visit the [ReductStore HTTP API documentation](https://reduct.store/docs/http-api).