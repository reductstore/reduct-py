"""Unit tests for lifecycle models."""

from reduct import LifecycleMode, LifecycleSettings, LifecycleType


def test__lifecycle_settings_support_compress_type_and_older_than():
    """LifecycleSettings should expose the new compress action and older_than field."""
    settings = LifecycleSettings(
        bucket="bucket-1",
        older_than="1h",
        type=LifecycleType.COMPRESS,
    )

    assert settings.type == LifecycleType.COMPRESS
    assert settings.older_than == "1h"
    assert settings.mode == LifecycleMode.ENABLED
    assert settings.model_dump() == {
        "type": LifecycleType.COMPRESS,
        "bucket": "bucket-1",
        "entries": [],
        "older_than": "1h",
        "interval": None,
        "when": None,
        "mode": LifecycleMode.ENABLED,
    }
