# Python implementation of Reduct Storage HTTP API
# (c) 2022 Alexey Timin
import enum
import time
from typing import Optional, List, Tuple, AsyncIterator

from pydantic import AnyHttpUrl, BaseModel


class QuotaType(enum):
    NONE = "NONE"
    FIFO = "FIFO"


class BucketSettings(BaseModel):
    max_block_size: Optional[int]
    quota_type: Optional[QuotaType]
    quota_size: Optional[int]


class Bucket:

    async def read(self, entry_name: str, timestamp: float) -> bytes:
        pass

    async def write(self, entry_name: str, data: bytes, timestamp=time.time()):
        pass

    async def list(self, entry_name: str, start: float, stop: float) -> List[Tuple[float, int]]:
        pass

    async def walk(self, entry_name: str, start: float, stop: float) -> AsyncIterator[bytes]:
        pass

    async def remove(self):
        pass


class ServerInfo(BaseModel):
    version: str
    bucket_count: int


class Client:

    def __init__(self, url: AnyHttpUrl):
        self.url = url
        pass

    async def info(self) -> ServerInfo:
        pass

    async def get_bucket(self, name: str) -> Bucket:
        pass

    async def create_bucket(self, name: str, settings: Optional[BucketSettings]) -> Bucket:
        pass
