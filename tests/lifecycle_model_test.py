"""Unit tests for lifecycle message models."""

from datetime import datetime

from reduct import LifecycleInfo, LifecycleMode, LifecycleSettings, LifecycleType


def test__lifecycle_info_parses_type_and_last_run():
    """Lifecycle info should parse the new type and datetime fields."""
    lifecycle = LifecycleInfo.model_validate(
        {
            "name": "test",
            "is_provisioned": False,
            "is_running": True,
            "type": "compress",
            "mode": "enabled",
            "last_run": "2026-06-15T07:02:25Z",
        }
    )

    assert lifecycle.type == LifecycleType.COMPRESS
    assert lifecycle.mode == LifecycleMode.ENABLED
    assert lifecycle.last_run == datetime.fromisoformat("2026-06-15T07:02:25+00:00")


def test__lifecycle_info_defaults_when_server_omits_new_fields():
    """Lifecycle info should remain backward compatible with older servers."""
    lifecycle = LifecycleInfo.model_validate(
        {
            "name": "test",
            "is_provisioned": False,
            "is_running": True,
        }
    )

    assert lifecycle.type == LifecycleType.DELETE
    assert lifecycle.mode == LifecycleMode.ENABLED
    assert lifecycle.last_run is None


def test__lifecycle_settings_support_compress_type():
    """Lifecycle settings should allow the new compress action."""
    settings = LifecycleSettings(
        bucket="bucket-1",
        older_than="1h",
        type=LifecycleType.COMPRESS,
    )

    assert settings.type == LifecycleType.COMPRESS
