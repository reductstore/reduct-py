"""Main client code"""
# Python implementation of Reduct Storage HTTP API
# (c) 2022 Alexey Timin
import json
import time
from enum import Enum
from typing import Optional, List, Tuple, AsyncIterator

import aiohttp
from pydantic import AnyHttpUrl, BaseModel


class ServerError(BaseModel):
    """sent from the server"""

    detail: str


class ReductError(Exception):
    """general exception for all errors"""

    def __init__(self, code, message):
        self._code = code
        self._detail = message
        self.message = ServerError.parse_raw(message)
        super().__init__(self.message)


class QuotaType(Enum):
    """determines if database has fixed size"""

    NONE = "NONE"
    FIFO = "FIFO"


class BucketSettings(BaseModel):
    """configuration for the currently connected db"""

    max_block_size: Optional[int]
    quota_type: Optional[QuotaType]
    quota_size: Optional[int]


class Entry(BaseModel):
    """single object with internal times"""

    name: str
    size: int
    block_count: int
    record_count: int
    oldest_record: int
    latest_record: int


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


class BucketInfo(BaseModel):
    """returned by '/list' endpoint - info about each bucket"""

    name: str
    entry_count: int
    size: int
    oldest_record: int
    latest_record: int


class BucketList(BaseModel):
    """multiple buckets"""

    buckets: List[BucketInfo]


class BucketEntries(BaseModel):
    """information about bucket and contained entries"""

    settings: BucketSettings
    info: BucketInfo
    entries: List[Entry]


class Bucket:
    """top level storage object"""

    def __init__(
        self,
        bucket_url: AnyHttpUrl,
        bucket_name: str,
        settings: Optional[BucketSettings] = None,
    ):
        self.bucket_url = bucket_url
        self.bucket_name = bucket_name
        self.settings = settings

    async def read(self, entry_name: str, timestamp: float) -> bytes:
        """read an object from the db"""
        params = {"ts": timestamp}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}", params=params
            ) as response:
                if response.ok:
                    return await response.text()
                raise ReductError(response.status, await response.read())

    async def write(self, entry_name: str, data: bytes, timestamp=time.time()):
        """write an object to db"""
        params = {"ts": timestamp}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}",
                params=params,
                data=data,
            ) as response:
                if not response.ok:
                    raise ReductError(response.status, await response.read())

    async def list(
        self, entry_name: str, start: float, stop: float
    ) -> List[Tuple[float, int]]:
        """list all objects in bucket"""
        params = {"start": start, "stop": stop}
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}/list",
                params=params,
            ) as response:
                if response.ok:
                    records = json.loads(await response.text())["records"]
                    items = [(record["ts"], record["size"]) for record in records]
                    return items
                raise ReductError(response.status, await response.read())

    async def walk(
        self, entry_name: str, start: float, stop: float
    ) -> AsyncIterator[bytes]:
        """step through all objects in a bucket"""
        items = await self.list(entry_name, start, stop)
        for timestamp, _ in items:
            data = await self.read(entry_name, timestamp)
            yield data

    async def remove(self):
        """not implemented in API yet?"""


class Client:
    """HTTP Client for Reduct Storage HTTP API"""

    def __init__(self, url: AnyHttpUrl):
        """
        Constructor

        Args:
            url: URL to connect to the storage

        Examples:
            >>> client = Client("http://127.0.0.1:8383")
            >>> info = await client.info()
            {
                "version": "0.5.0",
                "bucket_count": 2,
                "size": 1231825381,
                ...
            }
        """
        self.url = url.rstrip("/")

    async def info(self) -> ServerInfo:
        """
        Get high level server info

        Returns:
            ServerInfo:

        Raises:
            ReductError: if there is an HTTP error
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/info") as response:
                if response.ok:
                    return ServerInfo.parse_raw(await response.text())
                raise ReductError(response.status, await response.read())

    async def list(self) -> BucketList:
        """return a list of all buckets on server"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/list") as response:
                if response.ok:
                    return BucketList.parse_raw(await response.text())
                raise ReductError(response.status, await response.read())

    async def get_bucket(self, name: str) -> Bucket:
        """load a bucket to work with"""
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{self.url}/b/{name}") as response:
                if response.ok:
                    return Bucket(self.url, name)
                raise ReductError(response.status, await response.read())

    async def get_bucket_entries(self, name: str) -> BucketEntries:
        """load a bucket to work with"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/b/{name}") as response:
                if response.ok:
                    return BucketEntries.parse_raw(await response.text())
                raise ReductError(response.status, await response.read())

    async def create_bucket(
        self, name: str, settings: Optional[BucketSettings] = None
    ) -> Bucket:
        """create a new bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.url}/b/{name}", data=settings.json()
            ) as response:
                if response.ok:
                    return Bucket(self.url, name, settings)
                raise ReductError(response.status, await response.read())

    async def delete_bucket(self, name: str):
        """remove a bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self.url}/b/{name}") as response:
                if not response.ok:
                    raise ReductError(response.status, await response.read())

    async def update_bucket(self, name: str, settings: BucketSettings) -> bool:
        """update bucket settings"""
        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.url}/b/{name}", data=settings.json()
            ) as response:
                if response.ok:
                    return Bucket(self.url, name, settings)
                raise ReductError(response.status, await response.read())
