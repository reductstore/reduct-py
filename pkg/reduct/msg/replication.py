"""Message types for the Replication API"""

from typing import List, Dict, Optional

from pydantic import BaseModel, Field
from typing_extensions import deprecated


class ReplicationInfo(BaseModel):
    """Replication information"""

    name: str
    """name of the replication"""
    is_provisioned: bool
    """replication is provisioned and can't be deleted or changed"""
    is_active: bool
    """replication is active and the remote server is reachable"""
    pending_records: int
    """number of records to replicate"""


class ReplicationList(BaseModel):
    """List of replications"""

    replications: List[ReplicationInfo]
    """list of replications"""


class ReplicationDiagnosticsError(BaseModel):
    """Error information for replication"""

    count: int
    """number of times this error occurred"""
    last_message: str
    """last error message for this error"""


class ReplicationDiagnosticsDetail(BaseModel):
    """Diagnostics information for replication"""

    ok: int
    """number of successful replications"""
    errored: int
    """number of failed replications"""
    errors: Dict[int, ReplicationDiagnosticsError]
    """list of errors grouped by status code"""


class ReplicationDiagnostics(BaseModel):
    """Detailed diagnostics for replication"""

    hourly: ReplicationDiagnosticsDetail
    """hourly diagnostics"""


class ReplicationSettings(BaseModel):
    """Settings for creating a replication"""

    src_bucket: str
    """source bucket name"""
    dst_bucket: str
    """destination bucket name"""
    dst_host: str
    """url of the destination instance"""
    dst_token: Optional[str] = None
    """access token for the destination instance"""
    entries: List[str] = Field([])
    """list of entries to replicate. If empty, all entries are replicated.
    Wildcards are supported"""

    each_s: Optional[float] = Field(
        None,
        deprecated=deprecated(
            "Use `$each_t` operator in `when` condition. "
            "It will be removed in v1.18.0."
        ),
    )
    """replicate a record every S seconds"""
    each_n: Optional[int] = Field(
        None,
        deprecated=deprecated(
            "Use `$each_n` operator in `when` condition. "
            "It will be removed in v1.18.0."
        ),
    )
    """replicate every Nth record"""
    when: Optional[Dict] = None


class ReplicationDetailInfo(BaseModel):
    """Complete information about a replication"""

    diagnostics: ReplicationDiagnostics
    """diagnostics information"""
    info: ReplicationInfo
    """replication information"""
    settings: ReplicationSettings
    """replication settings"""
