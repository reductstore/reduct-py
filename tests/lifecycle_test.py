"""Tests for lifecycle endpoints"""

import pytest
from reduct import (
    ReductError,
    LifecycleDetailInfo,
    LifecycleInfo,
    LifecycleMode,
    LifecycleSettings,
    LifecycleType,
)
from tests.conftest import requires_api


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test__get_lifecycles(client, lifecycle_1, lifecycle_2):
    """Test getting a list of lifecycle policies"""
    lifecycles = await client.get_lifecycles()
    assert isinstance(lifecycles, list)
    for lifecycle in [lifecycle_1, lifecycle_2]:
        assert lifecycle in [item.name for item in lifecycles]
        assert all(isinstance(item, LifecycleInfo) for item in lifecycles)
        assert all(item.mode in LifecycleMode for item in lifecycles)


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test__get_lifecycle_detail(client, lifecycle_1):
    """Test create a lifecycle policy and get its details"""
    lifecycle_detail = await client.get_lifecycle_detail(lifecycle_1)
    assert isinstance(lifecycle_detail, LifecycleDetailInfo)
    assert lifecycle_detail.info.name == lifecycle_1
    assert lifecycle_detail.settings.mode == LifecycleMode.ENABLED


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test__update_lifecycle(client, lifecycle_1, bucket_1):
    """Test updating an existing lifecycle policy"""
    new_settings = LifecycleSettings(
        bucket=bucket_1.name,
        older_than="2h",
        interval="20m",
        entries=["entry-1", "entry-2"],
        when={"&number": {"$gt": 1}},
    )
    await client.update_lifecycle(lifecycle_1, new_settings)
    lifecycle_detail = await client.get_lifecycle_detail(lifecycle_1)
    assert lifecycle_detail.settings.bucket == new_settings.bucket
    assert lifecycle_detail.settings.older_than == new_settings.older_than
    assert lifecycle_detail.settings.interval == new_settings.interval


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test_delete_lifecycle(client, temporary_lifecycle):
    """Test deleting a lifecycle policy"""
    await client.delete_lifecycle(temporary_lifecycle)
    with pytest.raises(ReductError) as reduct_err:
        await client.get_lifecycle_detail(temporary_lifecycle)
    assert (
        str(reduct_err.value)
        == f"Status 404: Lifecycle '{temporary_lifecycle}' does not exist"
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test__lifecycle_with_when(client, random_prefix, bucket_1):
    """Test creating a lifecycle policy with when condition"""
    lifecycle_name = f"{random_prefix}-lifecycle-when"
    settings = LifecycleSettings(
        bucket=bucket_1.name,
        older_than="1h",
        interval="10m",
        when={"&number": {"$gt": 1}},
    )

    await client.create_lifecycle(lifecycle_name, settings)
    lifecycle = await client.get_lifecycle_detail(lifecycle_name)

    assert lifecycle.settings.when == {"&number": {"$gt": 1}}


@pytest.mark.asyncio
@pytest.mark.usefixtures("bucket_1")
@requires_api("1.20")
async def test__set_lifecycle_mode(client, lifecycle_1):
    """Test updating lifecycle mode without touching settings"""
    await client.set_lifecycle_mode(lifecycle_1, LifecycleMode.DRY_RUN)
    lifecycle_detail = await client.get_lifecycle_detail(lifecycle_1)

    assert lifecycle_detail.info.mode == LifecycleMode.DRY_RUN
    assert lifecycle_detail.settings.mode == LifecycleMode.DRY_RUN


def test__lifecycle_settings_support_compress_type():
    """Test lifecycle compress action is exposed by the SDK."""
    settings = LifecycleSettings(
        bucket="bucket-1",
        older_than="1h",
        type=LifecycleType.COMPRESS,
    )

    assert settings.type == LifecycleType.COMPRESS
    assert settings.model_dump()["type"] == LifecycleType.COMPRESS
