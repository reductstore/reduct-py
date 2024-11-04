"""Message types for the Replication API"""

from typing import List, Dict, Optional

from pydantic import BaseModel


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
    dst_token: str = ""
    """access token for the destination instance"""
    entries: List[str] = []
    """list of entries to replicate. If empty, all entries are replicated.
    Wildcards are supported"""
    include: Dict[str, str] = {}
    """replicate only records with these labels"""
    exclude: Dict[str, str] = {}
    """exclude records with these labels"""
    each_s: Optional[float] = None
    """replicate a record every S seconds"""
    each_n: Optional[int] = None
    """replicate every Nth record"""


class ReplicationDetailInfo(BaseModel):
    """Complete information about a replication"""

    diagnostics: ReplicationDiagnostics
    """diagnostics information"""
    info: ReplicationInfo
    """replication information"""
    settings: ReplicationSettings
    """replication settings"""
