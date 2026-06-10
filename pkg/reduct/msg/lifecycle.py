"""Message types for the Lifecycle API"""

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class LifecycleType(str, Enum):
    """Lifecycle action type"""

    DELETE = "delete"
    COMPRESS = "compress"


class LifecycleMode(str, Enum):
    """Lifecycle mode"""

    ENABLED = "enabled"
    DISABLED = "disabled"
    DRY_RUN = "dry_run"


class LifecycleInfo(BaseModel):
    """Lifecycle information"""

    name: str
    """name of the lifecycle policy"""
    is_provisioned: bool
    """lifecycle policy is provisioned and can't be deleted or changed"""
    is_running: bool
    """lifecycle worker is running"""
    mode: LifecycleMode = LifecycleMode.ENABLED
    """current lifecycle mode"""


class LifecycleList(BaseModel):
    """List of lifecycle policies"""

    lifecycles: List[LifecycleInfo]
    """list of lifecycle policies"""


class LifecycleSettings(BaseModel):
    """Settings for creating a lifecycle policy"""

    type: LifecycleType = LifecycleType.DELETE
    """lifecycle action type"""
    bucket: str
    """bucket to apply lifecycle policy"""
    entries: List[str] = Field(default_factory=list)
    """list of entries to process. If empty, all matching entries are used"""
    older_than: str
    """process records older than this duration"""
    interval: Optional[str] = None
    """interval between lifecycle runs"""
    when: Optional[Dict] = None
    """conditional query"""
    mode: LifecycleMode = LifecycleMode.ENABLED
    """lifecycle mode"""


class LifecycleDetailInfo(BaseModel):
    """Complete information about a lifecycle policy"""

    info: LifecycleInfo
    """lifecycle information"""
    settings: LifecycleSettings
    """lifecycle settings"""
