"""Message types for the Bucket API"""

from enum import Enum
from typing import Optional, List, Dict

from pydantic import BaseModel


class QuotaType(Enum):
    """determines if database has a fixed size"""

    NONE = "NONE"
    FIFO = "FIFO"
    HARD = "HARD"


class BucketSettings(BaseModel):
    """Configuration for a bucket"""

    max_block_size: Optional[int] = None
    """max block size in bytes"""

    max_block_records: Optional[int] = None
    """max number of records in a block"""

    quota_type: Optional[QuotaType] = None
    """quota type"""

    quota_size: Optional[int] = None
    """quota size in bytes"""


class BucketInfo(BaseModel):
    """Information about each bucket"""

    name: str
    """name of bucket"""

    entry_count: int
    """number of entries in the bucket"""

    size: int
    """size of bucket data in bytes"""

    oldest_record: int
    """UNIX timestamp of the oldest record in microseconds"""

    latest_record: int
    """UNIX timestamp of the latest record in microseconds"""

    is_provisioned: bool = False
    """bucket is provisioned amd you can't remove it or change its settings"""


class EntryInfo(BaseModel):
    """Entry of bucket"""

    name: str
    """name of entry"""

    size: int
    """size of stored data in bytes"""

    block_count: int
    """number of blocks"""

    record_count: int
    """number of records"""
    oldest_record: int

    """UNIX timestamp of the oldest record in microseconds"""

    latest_record: int
    """UNIX timestamp of the latest record in microseconds"""


class BucketFullInfo(BaseModel):
    """Information about bucket and contained entries"""

    info: BucketInfo
    """statistics about bucket"""

    settings: BucketSettings
    """settings of bucket"""

    entries: List[EntryInfo]
    """information about entries of bucket"""


class QueryType(Enum):
    """Query types"""

    QUERY = "QUERY"
    """query entries"""

    REMOVE = "REMOVE"
    """remove entries"""


class QueryEntry(BaseModel):
    """Query entry"""

    query_type: QueryType
    """query type"""

    start: Optional[int] = None
    """start time in microseconds"""

    stop: Optional[int] = None
    """end time in microseconds"""

    each_s: Optional[float] = None
    """return a record every S seconds"""

    each_n: Optional[int] = None
    """return every Nth record"""

    limit: Optional[int] = None
    """limit the number of records returned"""

    ttl: Optional[int] = None
    """time to live of the query in seconds"""

    only_metadata: Optional[bool] = None
    """return only metadata"""

    continuous: Optional[bool] = None
    """continuous query, it doesn't stop until the TTL is reached"""

    when: Optional[Dict] = None
    """conditional query to filter records"""

    strict: Optional[bool] = None
    """strict mode for when clause"""

    ext: Optional[Dict[str, Dict]] = None
    """additional parameters for extensions"""


class CreateQueryLinkRequest(BaseModel):
    """Parameters for creating a query link"""

    bucket: str
    """bucket name"""
    entry: str
    """entry name"""
    index: Optional[int] = None
    """record index"""
    query: QueryEntry
    """query"""
    expire_at: int = None
    """expiration time as UNIX timestamp in seconds"""


class CreateQueryLinkResponse(BaseModel):
    """Response from creating a query link"""

    link: str
    """query link"""
