"""Bucket API"""
from contextlib import asynccontextmanager
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import (
    Optional,
    List,
    AsyncIterator,
    Union,
    Callable,
    Awaitable,
)

from pydantic import BaseModel

from reduct.http import HttpClient


class QuotaType(Enum):
    """determines if database has a fixed size"""

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


@dataclass
class Record:
    """Record in a query"""

    timestamp: int
    """UNIX timestamp in microseconds"""
    size: int
    """size of data"""
    last: bool
    """last record in the query"""
    read_all: Callable[[None], Awaitable[bytes]]
    """read all data"""
    read: Callable[[int], AsyncIterator[bytes]]
    """read data in chunks"""


class Bucket:
    """A bucket of data in Reduct Storage"""

    def __init__(self, name: str, http: HttpClient):
        self._http = http
        self.name = name

    async def get_settings(self) -> BucketSettings:
        """
        Get current bucket settings
        Returns:
             BucketSettings:
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).settings

    async def set_settings(self, settings: BucketSettings):
        """
        Update bucket settings
        Args:
            settings: new settings
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("PUT", f"/b/{self.name}", data=settings.json())

    async def info(self) -> BucketInfo:
        """
        Get statistics about bucket
        Returns:
           BucketInfo:
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).info

    async def get_entry_list(self) -> List[EntryInfo]:
        """
        Get list of entries with their stats
        Returns:
            List[EntryInfo]
        Raises:
            ReductError: if there is an HTTP error
        """
        return (await self.get_full_info()).entries

    async def remove(self):
        """
        Remove bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/b/{self.name}")

    @asynccontextmanager
    async def read(
        self, entry_name: str, timestamp: Optional[int] = None
    ) -> AsyncIterator[Record]:
        """
        Read a record from entry
        Args:
            entry_name: name of entry in the bucket
            timestamp: UNIX timestamp in microseconds - if None: get the latest record
        Returns:
            async context, which generates Records
        Raises:
            ReductError: if there is an HTTP error
        Examples:
            >>> async def reader():
            >>>     async with bucket.read("entry", timestamp=123456789) as record:
            >>>         data = await record.read_all()
        """
        params = {"ts": timestamp} if timestamp else None
        async with self._http.request(
            "GET", f"/b/{self.name}/{entry_name}", params=params
        ) as resp:
            timestamp = int(resp.headers["x-reduct-time"])
            size = int(resp.headers["content-length"])

            yield Record(
                timestamp=timestamp,
                size=size,
                last=True,
                read_all=resp.read,
                read=resp.content.iter_chunked,
            )

    async def write(
        self,
        entry_name: str,
        data: Union[bytes, AsyncIterator[bytes]],
        timestamp: Optional[int] = None,
        content_length: Optional[int] = None,
    ):
        """
        Write a record to entry

        Args:
            entry_name: name of entry in the bucket
            data: bytes to write or async iterator
            timestamp: UNIX time stamp in microseconds. Current time if it's None
            content_length: content size in bytes,
                needed only when the data is an iterator
        Raises:
            ReductError: if there is an HTTP error

        Examples:
            >>> await bucket.write("entry-1", b"some_data", timestamp=19231023101)
            >>>
            >>> # You can write data chunk-wise using an asynchronous iterator and the
            >>> # size of the content:
            >>>
            >>> async def sender():
            >>>     for chunk in [b"part1", b"part2", b"part3"]:
            >>>         yield chunk
            >>> await bucket.write("entry-1", sender(), content_length=15)

        """
        params = {"ts": timestamp if timestamp else time.time_ns() / 1000}

        await self._http.request_all(
            "POST",
            f"/b/{self.name}/{entry_name}",
            params=params,
            data=data,
            content_length=content_length if content_length is not None else len(data),
        )

    async def query(
        self,
        entry_name: str,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        ttl: Optional[int] = None,
    ) -> AsyncIterator[Record]:
        """
        Query data for a time interval

        Args:
            entry_name: name of entry in the bucket
            start: the beginning of the time interval
            stop: the end of the time interval
            ttl: Time To Live of the request in seconds

        Returns:
             AsyncIterator[Record]: iterator to the records

        Examples:
            >>> async for record in bucket.query("entry-1", stop=time.time_ns() / 1000):
            >>>     data: bytes = record.read_all()
            >>>     # or
            >>>     async for chunk in record.read(n=1024):
            >>>         print(chunk)
        """
        params = {}
        if start:
            params["start"] = start
        if stop:
            params["stop"] = stop
        if ttl:
            params["ttl"] = ttl

        url = f"/b/{self.name}/{entry_name}"
        data = await self._http.request_all(
            "GET",
            f"{url}/q",
            params=params,
        )
        query_id = json.loads(data)["id"]
        last = False
        while not last:
            async with self._http.request("GET", f"{url}?q={query_id}") as resp:
                if resp.status == 202:
                    return

                timestamp = int(resp.headers["x-reduct-time"])
                size = int(resp.headers["content-length"])
                last = int(resp.headers["x-reduct-last"]) != 0

                yield Record(
                    timestamp=timestamp,
                    size=size,
                    last=last,
                    read_all=resp.read,
                    read=resp.content.iter_chunked,
                )

    async def get_full_info(self) -> BucketFullInfo:
        """
        Get full information about bucket (settings, statistics, entries)
        """
        return BucketFullInfo.parse_raw(
            await self._http.request_all("GET", f"/b/{self.name}")
        )
