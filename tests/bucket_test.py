"""Tests for Bucket"""
import pytest

from reduct import ReductError, BucketSettings


@pytest.mark.asyncio
async def test__remove_ok(client):
    """Should remove a bucket"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await client.get_bucket("bucket")


@pytest.mark.asyncio
async def test__remove_not_exist(client):
    """Should not remove a bucket if it doesn't exist"""
    bucket = await client.create_bucket("bucket")
    await bucket.remove()
    with pytest.raises(ReductError):
        await bucket.remove()


@pytest.mark.asyncio
async def test__set_settings(bucket_1):
    """Should set new settings"""
    await bucket_1.set_settings(BucketSettings(max_block_size=10000))
    new_settings = await bucket_1.get_settings()
    assert new_settings.dict() == {
        "max_block_size": 10000,
        "quota_size": None,
        "quota_type": None,
    }  # actually bug
