"""Message types for the Server API"""

from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel

from reduct.msg.bucket import BucketSettings, BucketInfo


class Defaults(BaseModel):
    """Default server settings"""

    bucket: BucketSettings
    """settings for a new bucket"""


class LicenseInfo(BaseModel):
    """License information"""

    licensee: str
    """Licensee usually is the company name"""

    invoice: str
    """Invoice number"""

    expiry_date: datetime
    """Expiry date of the license in ISO 8601 format (UTC)"""

    plan: str
    """Plan name"""

    device_number: int
    """Number of devices (0 for unlimited)"""

    disk_quota: int
    """Disk quota in TB (0 for unlimited)"""

    fingerprint: str
    """License fingerprint"""


class ServerInfo(BaseModel):
    """Server stats"""

    version: str
    """version of the storage in x.y.z format"""

    bucket_count: int
    """number of buckets in the storage"""

    usage: int
    """stored data in bytes"""

    uptime: int
    """storage uptime in seconds"""

    oldest_record: int
    """UNIX timestamp of the oldest record in microseconds"""

    latest_record: int
    """UNIX timestamp of the latest record in microseconds"""

    license: Optional[LicenseInfo] = None
    """license information"""

    defaults: Defaults
    """Default server settings"""


class BucketList(BaseModel):
    """List of buckets"""

    buckets: List[BucketInfo]
    """list of buckets"""
