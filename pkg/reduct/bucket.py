"""Bucket API"""
import json
from enum import Enum
from typing import Optional, List, Tuple
import time

from pydantic import BaseModel

from reduct.http import request


class QuotaType(Enum):
    """determines if database has fixed size"""

    NONE = "NONE"
    FIFO = "FIFO"


class BucketSettings(BaseModel):
    """configuration for the currently connected db"""

    max_block_size: Optional[int]
    quota_type: Optional[QuotaType]
    quota_size: Optional[int]


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


class Entry(BaseModel):
    """single object with internal times"""

    name: str
    size: int
    block_count: int
    record_count: int
    oldest_record: int
    latest_record: int


class BucketEntries(BaseModel):
    """information about bucket and contained entries"""

    settings: BucketSettings
    info: BucketInfo
    entries: List[Entry]


def _us(timestamp: float) -> int:
    return int(timestamp * 1_000_000)


class Bucket:
    """top level storage object"""

    def __init__(
        self,
        bucket_url: str,
        bucket_name: str,
        settings: Optional[BucketSettings] = None,
    ):
        self.bucket_url = bucket_url
        self.bucket_name = bucket_name
        self.settings = settings

    async def read(self, entry_name: str, timestamp: float) -> bytes:
        """read an object from the db"""
        params = {"ts": _us(timestamp)}
        return await request(
            "GET", f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}", params=params
        )

    async def write(self, entry_name: str, data: bytes, timestamp=time.time()):
        """write an object to db"""
        params = {"ts": _us(timestamp)}

        await request(
            "POST",
            f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}",
            params=params,
            data=data,
        )

    async def list(
        self, entry_name: str, start: float, stop: float
    ) -> List[Tuple[float, int]]:
        """list all objects in bucket"""
        params = {"start": _us(start), "stop": _us(stop)}
        data = await request(
            "GET",
            f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}/list",
            params=params,
        )
        records = json.loads(data)["records"]
        items = [(record["ts"], record["size"]) for record in records]
        return items

    async def remove(self):
        """not implemented in API yet?"""
