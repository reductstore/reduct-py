"""Main client code"""
from typing import Optional, List

from pydantic import BaseModel

from reduct.bucket import BucketInfo, BucketSettings, Bucket
from reduct.http import HttpClient
from reduct.error import ReductError


class Defaults(BaseModel):
    """Default server settings"""

    bucket: BucketSettings
    """settings for a new bucket"""


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

    defaults: Defaults
    """Default server settings"""


class BucketList(BaseModel):
    """List of buckets"""

    buckets: List[BucketInfo]


class Client:
    """HTTP Client for Reduct Storage HTTP API"""

    def __init__(
        self, url: str, api_token: Optional[str] = None, timeout: Optional[float] = None
    ):
        """
        Constructor

        Args:
            url: URL to connect to the storage
            api_token: API token if the storage uses it for authorization
            timeout: total timeout for connection, request and response in seconds

        Examples:
            >>> client = Client("http://127.0.0.1:8383")
            >>> info = await client.info()
        """
        self._http = HttpClient(url.rstrip("/"), api_token, timeout)

    async def info(self) -> ServerInfo:
        """
        Get high level server info

        Returns:
            ServerInfo:

        Raises:
            ReductError: if there is an HTTP error
        """
        return ServerInfo.parse_raw(await self._http.request_all("GET", "/info"))

    async def list(self) -> List[BucketInfo]:
        """
        Return a list of all buckets on server

        Returns:
            List[BucketInfo]
        Raises:
            ReductError: if there is an HTTP error
        """
        return BucketList.parse_raw(
            await self._http.request_all("GET", "/list")
        ).buckets

    async def get_bucket(self, name: str) -> Bucket:
        """
        Load a bucket to work with
        Args:
            name: name of the bucket
        Returns:
            Bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("HEAD", f"/b/{name}")
        return Bucket(name, self._http)

    async def create_bucket(
        self, name: str, settings: Optional[BucketSettings] = None, exist_ok=False
    ) -> Bucket:
        """
        Create a new bucket
        Args:
            name: a name for the bucket
            settings: settings for the bucket If None, the server
            default settings is used.
            exist_ok: the client raises no exception if the bucket
            already exists and returns it
        Returns:
            Bucket: created bucket
        Raises:
            ReductError: if there is an HTTP error
        """
        data = settings.json() if settings else None
        try:
            await self._http.request_all("POST", f"/b/{name}", data=data)
        except ReductError as err:
            if err.status_code != 409 or not exist_ok:
                raise err

        return Bucket(name, self._http)
