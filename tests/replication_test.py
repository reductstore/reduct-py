"""Tests for replication endpoints"""

from contextlib import suppress

import pytest
from reduct import (
    ReductError,
    ReplicationCompression,
    ReplicationDetailInfo,
    ReplicationInfo,
    ReplicationMode,
    ReplicationSettings,
)
from tests.conftest import requires_api


async def _delete_replication_if_exists(client, replication_name):
    with suppress(ReductError):
        await client.delete_replication(replication_name)


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__get_replications(client, replication_1, replication_2):
    """Test getting a list of replications"""
    replications = await client.get_replications()
    assert isinstance(replications, list)
    for replication in [replication_1, replication_2]:
        assert replication in [repl.name for repl in replications]
        assert all(isinstance(repl, ReplicationInfo) for repl in replications)
        assert all(repl.mode in ReplicationMode for repl in replications)


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__get_replication_detail(client, replication_1):
    """Test create a replication and get its details"""
    replication_detail = await client.get_replication_detail(replication_1)
    assert isinstance(replication_detail, ReplicationDetailInfo)
    assert replication_detail.info.name == replication_1
    assert replication_detail.settings.mode == ReplicationMode.ENABLED


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test__update_replication(client, replication_1, bucket_1, bucket_2):
    """Test updating an existing replication"""
    new_settings = ReplicationSettings(
        src_bucket=bucket_2.name,
        dst_bucket=bucket_1.name,
        dst_host="https://play.reduct.store",
    )
    await client.update_replication(replication_1, new_settings)
    replication_detail = await client.get_replication_detail(replication_1)
    assert replication_detail.settings.src_bucket == new_settings.src_bucket
    assert replication_detail.settings.dst_bucket == new_settings.dst_bucket
    assert replication_detail.settings.dst_host == new_settings.dst_host


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
async def test_delete_replication(client, temporary_replication):
    """Test deleting a replication"""
    await client.delete_replication(temporary_replication)
    with pytest.raises(ReductError) as reduct_err:
        await client.get_replication_detail(temporary_replication)
    assert (
        str(reduct_err.value)
        == f"Status 404: Replication '{temporary_replication}' does not exist"
    )


@pytest.mark.asyncio
async def test__replication_with_when(client, random_prefix, bucket_1, bucket_2):
    """Test creating a replication with when condition"""
    replication_name = f"{random_prefix}-replication-when"
    settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="https://play.reduct.store",
        when={"&number": {"$gt": 1}},
    )

    await client.create_replication(replication_name, settings)
    replication = await client.get_replication_detail(replication_name)

    assert replication.settings.when == {"&number": {"$gt": 1}}


@pytest.mark.asyncio
@requires_api("1.21")
async def test__replication_with_prefix(client, random_prefix, bucket_1, bucket_2):
    """Test creating and updating a replication with destination prefix"""
    replication_name = f"{random_prefix}-replication-prefix"
    settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="http://127.0.0.1:8383",
        dst_prefix="robot-1",
    )

    try:
        await client.create_replication(replication_name, settings)
        replication = await client.get_replication_detail(replication_name)

        assert replication.settings.dst_prefix == "robot-1"

        settings.dst_prefix = "line-a"
        await client.update_replication(replication_name, settings)
        replication = await client.get_replication_detail(replication_name)

        assert replication.settings.dst_prefix == "line-a"
    finally:
        await _delete_replication_if_exists(client, replication_name)


@pytest.mark.asyncio
@requires_api("1.21")
async def test__replication_with_compression(client, random_prefix, bucket_1, bucket_2):
    """Test creating and updating a replication with compression"""
    replication_name = f"{random_prefix}-replication-compression"
    settings = ReplicationSettings(
        src_bucket=bucket_1.name,
        dst_bucket=bucket_2.name,
        dst_host="http://127.0.0.1:8383",
        compression=ReplicationCompression.ZSTD,
    )

    try:
        await client.create_replication(replication_name, settings)
        replication = await client.get_replication_detail(replication_name)

        assert replication.settings.compression == ReplicationCompression.ZSTD

        settings.compression = ReplicationCompression.GZIP
        await client.update_replication(replication_name, settings)
        replication = await client.get_replication_detail(replication_name)

        assert replication.settings.compression == ReplicationCompression.GZIP
    finally:
        await _delete_replication_if_exists(client, replication_name)


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1", "bucket_2")
@requires_api("1.18")
async def test__set_replication_mode(client, replication_1):
    """Test updating replication mode without touching settings"""
    await client.set_replication_mode(replication_1, ReplicationMode.PAUSED)
    replication_detail = await client.get_replication_detail(replication_1)

    assert replication_detail.info.mode == ReplicationMode.PAUSED
    assert replication_detail.settings.mode == ReplicationMode.PAUSED
