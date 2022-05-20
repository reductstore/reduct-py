"""Bucket API"""
import json
from enum import Enum
from typing import Optional, List, Tuple
import time

from pydantic import BaseModel

from reduct.http import HttpClient


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


class Bucket:
    """A bucket of data in Reduct Storage"""

    def __init__(self, name: str, http: HttpClient):
        self._http = http
        self.name = name

    async def get_settings(self) -> BucketSettings:
        """
        Get current settings of bucket
        Returns:
             BucketSettings:
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.__get_full_info()).settings

    async def set_settings(self, settings: BucketSettings):
        """
        Update bucket settings
        Args:
            settings: new settings
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request("PUT", f"/b/{self.name}", data=settings.json())

    async def info(self) -> BucketInfo:
        """
        Get statistics about bucket
        Returns:
           BucketInfo:
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.__get_full_info()).info

    async def get_entry_list(self) -> List[EntryInfo]:
        """
        Get list of entries with its stats
        Returns:
            List[EntryInfo]
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.__get_full_info()).entries

    async def remove(self):
        """
        Remove bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request("DELETE", f"/b/{self.name}")

    async def read(self, entry_name: str, timestamp: Optional[int] = None) -> bytes:
        """
        Read a record from entry
        Args:
            entry_name: name of entry in the bucket
            timestamp: UNIX timestamp in microseconds if None get the latest record
        Returns:
            bytes:
        Raises:
            ReductError: if there is an HTTP error
        """
        params = {"ts": timestamp} if timestamp else None
        return await self._http.request(
            "GET", f"/b/{self.name}/{entry_name}", params=params
        )

    async def write(
        self, entry_name: str, data: bytes, timestamp: Optional[int] = None
    ):
        """
        Write a record to entry
        Args:
            entry_name: name of entry in the bucket
            data: data to write
            timestamp: UNIX timestamp in microseconds. Current time if it's None
        Raises:
            ReductError: if there is an HTTP error

        """
        params = {"ts": timestamp if timestamp else time.time_ns() / 1000}

        await self._http.request(
            "POST",
            f"/b/{self.name}/{entry_name}",
            params=params,
            data=data,
        )

    async def list(
        self, entry_name: str, start: int, stop: int
    ) -> List[Tuple[int, int]]:
        """
        Get list of records in entry for time interval
        Args:
            entry_name: name of entry in the bucket
            start: the beginning of the time interval
            stop: the end of the time interval
        """
        params = {"start": start, "stop": stop}
        data = await self._http.request(
            "GET",
            f"/b/{self.name}/{entry_name}/list",
            params=params,
        )
        records = json.loads(data)["records"]
        items = [(int(record["ts"]), int(record["size"])) for record in records]
        return items

    async def __get_full_info(self) -> BucketFullInfo:
        return BucketFullInfo.parse_raw(
            await self._http.request("GET", f"/b/{self.name}")
        )
