"""Main client code"""
# Python implementation of Reduct Storage HTTP API
# (c) 2022 Alexey Timin
import json
import time
from enum import Enum
from typing import Optional, List, Tuple, AsyncIterator

import aiohttp
from pydantic import AnyHttpUrl, BaseModel


class ReductError(Exception):
    """general exception for all errors"""

    def __init__(self, code, detail):
        self._code = code
        self._detail = detail
        self.message = f"server error: {self._detail} - code: {self._code}"
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
                if response.status == 404:
                    raise ReductError(response.status, "cannot get - entry not found")
                if response.status == 422:
                    raise ReductError(response.status, "cannot get - bad timestamps")

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
                    raise ReductError(response.status, "could not write")

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
                if response.status == 200:
                    records = json.loads(await response.text())["records"]
                    items = [(record["ts"], record["size"]) for record in records]
                    return items
                if response.status == 422:
                    raise ReductError(response.status, "cannot list - bad timestamps")
                else:
                    raise ReductError(response.status, "cannot list - unknown error")

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


class ServerInfo(BaseModel):
    """server stats"""

    version: str
    bucket_count: int


class Client:
    """main connection to client"""

    def __init__(self, url: AnyHttpUrl):
        self.url = url

    async def info(self) -> ServerInfo:
        """get high level server info"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/info") as response:

                if response.ok:
                    info = json.loads(await response.text())
                    server_info = ServerInfo(**info)
                    return server_info
                raise ReductError(response.status, "cannot retrieve server info")

    async def get_bucket(self, name: str) -> Bucket:
        """load a bucket to work with"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/b/{name}") as response:
                if response.ok:
                    return Bucket(self.url, name)
                raise ReductError(response.status, "cannot get bucket")

    async def create_bucket(
        self, name: str, settings: Optional[BucketSettings] = None
    ) -> Bucket:
        """create a new bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.url}/b/{name}") as response:
                if response.ok:
                    return Bucket(self.url, name, settings)
                if response.status == 409:
                    raise ReductError(
                        response.status, "cannot create bucket - already exists"
                    )
                if response.status == 422:
                    raise ReductError(
                        response.status, "cannot create bucket - bad JSON"
                    )

    async def delete_bucket(self, name: str):
        """remove a bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self.url}/b/{name}") as response:
                if not response.ok:
                    raise ReductError(response.status, "cannot delete bucket")

    async def update_bucket(self, settings: BucketSettings) -> bool:
        """update bucket settings"""
