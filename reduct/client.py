"""Main client code"""
from datetime import datetime
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


class Permissions(BaseModel):
    """Token permission"""

    full_access: bool
    """full access to manage buckets and tokens"""

    read: Optional[List[str]]
    """list of buckets with read access"""

    write: Optional[List[str]]
    """list of buckets with write access"""


class Token(BaseModel):
    """Token for authentication"""

    name: str
    """name of token"""

    created_at: datetime
    """creation time of token"""


class FullTokenInfo(Token):
    """Full information about token with permissions"""

    permissions: Permissions
    """permissions of token"""


class TokenList(BaseModel):
    """List of tokens"""

    tokens: List[Token]


class TokenCreateResponse(BaseModel):
    """Response from creating a token"""

    value: str
    """token for authentication"""


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
        await self._http.request_all("GET", f"/b/{name}")
        return Bucket(name, self._http)

    async def create_bucket(
        self,
        name: str,
        settings: Optional[BucketSettings] = None,
        exist_ok: bool = False,
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

    async def get_token_list(self) -> List[Token]:
        """
        Get a list of all tokens
        Returns:
            List[Token]
        Raises:
            ReductError: if there is an HTTP error
        """
        return TokenList.parse_raw(
            await self._http.request_all("GET", "/tokens")
        ).tokens

    async def get_token(self, name: str) -> FullTokenInfo:
        """
        Get a token by name
        Args:
            name: name of the token
        Returns:
            Token
        Raises:
            ReductError: if there is an HTTP error
        """
        return FullTokenInfo.parse_raw(
            await self._http.request_all("GET", f"/tokens/{name}")
        )

    async def create_token(self, name: str, permissions: Permissions) -> str:
        """
        Create a new token
        Args:
            name: name of the token
            permissions: permissions for the token
        Returns:
            str: token value
        Raises:
            ReductError: if there is an HTTP error
        """
        return TokenCreateResponse.parse_raw(
            await self._http.request_all(
                "POST", f"/tokens/{name}", data=permissions.json()
            )
        ).value

    async def remove_token(self, name: str) -> None:
        """
        Delete a token
        Args:
            name: name of the token
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/tokens/{name}")

    async def me(self) -> FullTokenInfo:
        """
        Get information about the current token
        Returns:
            FullTokenInfo
        Raises:
            ReductError: if there is an HTTP error
        """
        return FullTokenInfo.parse_raw(await self._http.request_all("GET", "/me"))
