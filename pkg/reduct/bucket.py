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
    """Configuration for a bucket"""

    max_block_size: Optional[int]
    """max block size in bytes"""

    quota_type: Optional[QuotaType]
    """quota type"""

    quota_size: Optional[int]
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


class Entry(BaseModel):
    """single object with internal times"""

    name: str
    size: int
    block_count: int
    record_count: int
    oldest_record: int
    latest_record: int


class BucketFullInfo(BaseModel):
    """Information about bucket and contained entries"""

    info: BucketInfo
    """statistics about bucket"""

    settings: BucketSettings
    """settings of bucket"""

    entries: List[Entry]
    """information about entries of bucket"""


def _us(timestamp: float) -> int:
    return int(timestamp * 1_000_000)


class Bucket:
    """A bucket of data in Reduct Storage"""

    def __init__(
        self,
        server_url: str,
        name: str,
    ):
        self.server_url = server_url
        self.name = name

    async def get_settings(self) -> BucketSettings:
        """Get current settings of bucket"""
        return BucketFullInfo.parse_raw(
            await request("GET", f"{self.server_url}/b/{self.name}")
        ).settings

    async def set_settings(self, settings: BucketSettings):
        """update bucket settings"""
        await request("PUT", f"{self.server_url}/b/{self.name}", data=settings.json())

    async def info(self):
        pass

    async def get_entry_list(self):
        pass

    async def remove(self):
        """
        Remove bucket

        Raises:
            ReductError: if there is an HTTP error
        """
        await request("DELETE", f"{self.server_url}/b/{self.name}")

    async def read(self, entry_name: str, timestamp: float) -> bytes:
        """read an object from the db"""
        params = {"ts": _us(timestamp)}
        return await request(
            "GET", f"{self.server_url}/b/{self.name}/{entry_name}", params=params
        )

    async def write(self, entry_name: str, data: bytes, timestamp=time.time()):
        """write an object to db"""
        params = {"ts": _us(timestamp)}

        await request(
            "POST",
            f"{self.server_url}/b/{self.name}/{entry_name}",
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
            f"{self.server_url}/b/{self.name}/{entry_name}/list",
            params=params,
        )
        records = json.loads(data)["records"]
        items = [(record["ts"], record["size"]) for record in records]
        return items
