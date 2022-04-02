"""Main client code"""
# Python implementation of Reduct Storage HTTP API
# (c) 2022 Alexey Timin
import json
import time
import logging
from enum import Enum
from typing import Optional, List, Tuple, AsyncIterator

import aiohttp
from pydantic import AnyHttpUrl, BaseModel

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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
                return await response.text()

    async def write(self, entry_name: str, data: bytes, timestamp=time.time()):
        """write an object to db"""
        params = {"ts": timestamp}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.bucket_url}/b/{self.bucket_name}/{entry_name}",
                params=params,
                data=data,
            ) as response:
                if response.status != 200:
                    logger.error("error response from server: %d", response.status)

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
                elif response.status == 422:
                    logger.error(
                        "timestamps are bad - start: %d, stop: %d", start, stop
                    )
                else:
                    logger.error("unexpected status: %d", response.status)

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

    async def info(self) -> Optional[ServerInfo]:
        """get high level server info"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/info") as response:

                logger.debug("Status: %s", response.status)

                if response.status == 200:
                    info = json.loads(await response.text())
                    server_info = ServerInfo(**info)
                    logger.debug(server_info)
                    return server_info
                else:
                    logger.error("error: status code: %d", response.status)
                    return None

    async def get_bucket(self, name: str) -> Bucket:
        """load a bucket to work with"""
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.url}/b/{name}") as response:
                logger.debug("Status: %d", response.status)

                if response.status == 200:
                    resp = json.loads(await response.text())
                    print(resp)
                    return Bucket(self.url, name)

    async def create_bucket(
        self, name: str, settings: Optional[BucketSettings] = None
    ) -> Bucket:
        """create a new bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.url}/b/{name}") as response:
                logger.debug(response)
                if response.status == 200:
                    return Bucket(self.url, name, settings)
                if response.status == 409:
                    logger.error("bucket already exists")
                    return Bucket(self.url, name, settings)

    async def delete_bucket(self, name: str) -> bool:
        """remove a bucket"""
        async with aiohttp.ClientSession() as session:
            async with session.delete(f"{self.url}/b/{name}") as response:
                logger.debug(response)
                if response.status == 200:
                    return True
                if response.status == 404:
                    return False

    async def update_bucket(self, settings: BucketSettings) -> bool:
        """update bucket settings"""
