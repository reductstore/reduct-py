"""Bucket API"""
import asyncio
import json
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import (
    Optional,
    List,
    AsyncIterator,
    Union,
)

from pydantic import BaseModel

from reduct.http import HttpClient
from reduct.record import Record, parse_batched_records, parse_record


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

    async def remove_entry(self, entry_name: str):
        """
        Remove entry from bucket
        Args:
            entry_name: name of entry
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/b/{self.name}/{entry_name}")

    @asynccontextmanager
    async def read(
        self, entry_name: str, timestamp: Optional[int] = None, head: bool = False
    ) -> Record:
        """
        Read a record from entry
        Args:
            entry_name: name of entry in the bucket
            timestamp: UNIX timestamp in microseconds - if None: get the latest record
            head: if True: get only the header of a recod with metadata
        Returns:
            async context with a record
        Raises:
            ReductError: if there is an HTTP error
        Examples:
            >>> async def reader():
            >>>     async with bucket.read("entry", timestamp=123456789) as record:
            >>>         data = await record.read_all()
        """
        params = {"ts": int(timestamp)} if timestamp else None
        method = "HEAD" if head else "GET"
        async with self._http.request(
            method, f"/b/{self.name}/{entry_name}", params=params
        ) as resp:
            yield parse_record(resp)

    async def write(
        self,
        entry_name: str,
        data: Union[bytes, AsyncIterator[bytes]],
        timestamp: Optional[int] = None,
        content_length: Optional[int] = None,
        **kwargs,
    ):
        """
        Write a record to entry

        Args:
            entry_name: name of entry in the bucket
            data: bytes to write or async iterator
            timestamp: UNIX time stamp in microseconds. Current time if it's None
            content_length: content size in bytes,
                needed only when the data is an iterator
        Keyword Args:
            labels (dict): labels as key-values
            content_type (str): content type of data
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
        timestamp = timestamp if timestamp else time.time_ns() / 1000
        params = {"ts": int(timestamp)}
        await self._http.request_all(
            "POST",
            f"/b/{self.name}/{entry_name}",
            params=params,
            data=data,
            content_length=content_length if content_length is not None else len(data),
            **kwargs,
        )

    async def query(
        self,
        entry_name: str,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        ttl: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[Record]:
        """
        Query data for a time interval

        Args:
            entry_name: name of entry in the bucket
            start: the beginning of the time interval
            stop: the end of the time interval
            ttl: Time To Live of the request in seconds
        Keyword Args:
            include (dict): query records which have all labels from this dict
            exclude (dict): query records which doesn't have all labels from this
            head (bool): if True: get only the header of a recod with metadata
            limit (int): limit the number of records
        Returns:
             AsyncIterator[Record]: iterator to the records

        Examples:
            >>> async for record in bucket.query("entry-1", stop=time.time_ns() / 1000):
            >>>     data: bytes = record.read_all()
            >>>     # or
            >>>     async for chunk in record.read(n=1024):
            >>>         print(chunk)
        """
        query_id = await self._query(entry_name, start, stop, ttl, **kwargs)
        last = False
        method = "HEAD" if "head" in kwargs and kwargs["head"] else "GET"

        if self._http.api_version and self._http.api_version >= "1.5":
            while not last:
                async with self._http.request(
                    method, f"/b/{self.name}/{entry_name}/batch?q={query_id}"
                ) as resp:
                    if resp.status == 204:
                        return
                    async for record in parse_batched_records(resp):
                        last = record.last
                        yield record
        else:
            while not last:
                async with self._http.request(
                    method, f"/b/{self.name}/{entry_name}?q={query_id}"
                ) as resp:
                    if resp.status == 204:
                        return
                    last = int(resp.headers["x-reduct-last"]) != 0
                    yield parse_record(resp, last)

    async def get_full_info(self) -> BucketFullInfo:
        """
        Get full information about bucket (settings, statistics, entries)
        """
        return BucketFullInfo.parse_raw(
            await self._http.request_all("GET", f"/b/{self.name}")
        )

    async def subscribe(
        self, entry_name: str, start: Optional[int] = None, poll_interval=1.0, **kwargs
    ) -> AsyncIterator[Record]:
        """
        Query records from the start timestamp and wait for new records

        Args:
            entry_name: name of entry in the bucket
            start: the beginning timestamp to read records
            poll_interval: inteval to ask new records in seconds
        Keyword Args:
            include (dict): query records which have all labels from this dict
            exclude (dict): query records which doesn't have all labels from this dict
            head (bool): if True: get only the header of a recod with metadata
        Returns:
             AsyncIterator[Record]: iterator to the records

        Examples:
            >>> async for record in bucket.subscribes("entry-1"):
            >>>     data: bytes = record.read_all()
            >>>     # or
            >>>     async for chunk in record.read(n=1024):
            >>>         print(chunk)
        """
        query_id = await self._query(
            entry_name, start, None, poll_interval * 2 + 1, continuous=True, **kwargs
        )

        method = "HEAD" if "head" in kwargs and kwargs["head"] else "GET"
        if self._http.api_version and self._http.api_version >= "1.5":
            while True:
                async with self._http.request(
                    method, f"/b/{self.name}/{entry_name}/batch?q={query_id}"
                ) as resp:
                    if resp.status == 204:
                        await asyncio.sleep(poll_interval)
                        continue

                    async for record in parse_batched_records(resp):
                        yield record
        else:
            while True:
                async with self._http.request(
                    method, f"/b/{self.name}/{entry_name}?q={query_id}"
                ) as resp:
                    if resp.status == 204:
                        await asyncio.sleep(poll_interval)
                        continue

                    yield parse_record(resp, False)

    async def _query(self, entry_name, start, stop, ttl, **kwargs):
        params = {}
        if start:
            params["start"] = int(start)
        if stop:
            params["stop"] = int(stop)
        if ttl:
            params["ttl"] = int(ttl)

        if "include" in kwargs:
            for name, value in kwargs["include"].items():
                params[f"include-{name}"] = str(value)
        if "exclude" in kwargs:
            for name, value in kwargs["exclude"].items():
                params[f"exclude-{name}"] = str(value)

        if "continuous" in kwargs:
            params["continuous"] = "true" if kwargs["continuous"] else "false"

        if "limit" in kwargs:
            params["limit"] = kwargs["limit"]

        url = f"/b/{self.name}/{entry_name}"
        data = await self._http.request_all(
            "GET",
            f"{url}/q",
            params=params,
        )
        query_id = json.loads(data)["id"]
        return query_id
