"""Bucket API"""
import json
from enum import Enum
from typing import Optional, List, Tuple, AsyncIterator, Union
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

    max_block_records: Optional[int]
    """max number of records in a block"""

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
        blob = b""
        async for chunk in self.read_by(entry_name, timestamp):
            blob += chunk

        return blob

    async def read_by(
        self, entry_name: str, timestamp: Optional[int] = None, chunk_size: int = 1024
    ) -> AsyncIterator[bytes]:
        """
        Read a record from entry by chunks

        >>> async for chunk in bucket.read_by("entry-1", chunk_size=1024):
        >>>     print(chunk)
        Args:
            entry_name: name of entry in the bucket
            timestamp: UNIX timestamp in microseconds if None get the latest record
            chunk_size:
        Returns:
            bytes:
        Raises:
            ReductError: if there is an HTTP error
        """
        params = {"ts": timestamp} if timestamp else None
        async for chunk in self._http.request_by(
            "GET", f"/b/{self.name}/{entry_name}", params=params, chunk_size=chunk_size
        ):
            yield chunk

    async def write(
        self,
        entry_name: str,
        data: Union[bytes, Tuple[AsyncIterator[bytes], int]],
        timestamp: Optional[int] = None,
        content_length: Optional[int] = None,
    ):
        """
        Write a record to entry

        >>> await bucket.write("entry-1", b"some_data", timestamp=19231023101)

        You can writting data by chunks with an asynchronous iterator
        and size of content:

        >>> async def sender():
        >>>     for chunk in [b"part1", b"part2", b"part3"]:
        >>>         yield chunk
        >>> await bucket.write("entry-1", sender(), content_length=15)
        Args:
            entry_name: name of entry in the bucket
            data: bytes to write or async itterator
            timestamp: UNIX time stamp in microseconds. Current time if it's None
            content_length: content size in bytes,
                needed only when the data is itterator
        Raises:
            ReductError: if there is an HTTP error

        """
        params = {"ts": timestamp if timestamp else time.time_ns() / 1000}

        await self._http.request(
            "POST",
            f"/b/{self.name}/{entry_name}",
            params=params,
            data=data,
            content_length=content_length if content_length else len(data),
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
        Raises:
            ReductError: if there is an HTTP error
        Returns:
            List[Tuple[int,int]]:  list of tuples, where each tuple
            has time stamp (first element) of a record and its size in bytes
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
