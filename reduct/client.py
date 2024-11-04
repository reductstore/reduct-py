"""Client module for ReductStore HTTP API"""

from typing import Optional, List, Dict

from aiohttp import ClientSession

from reduct.bucket import BucketInfo, BucketSettings, Bucket
from reduct.error import ReductError
from reduct.http import HttpClient
from reduct.msg.replication import (
    ReplicationList,
    ReplicationDetailInfo,
    ReplicationSettings,
    ReplicationInfo,
)
from reduct.msg.server import ServerInfo, BucketList
from reduct.msg.token import (
    TokenCreateResponse,
    Token,
    TokenList,
    FullTokenInfo,
    Permissions,
)


class Client:
    """HTTP Client for Reduct Storage HTTP API"""

    def __init__(
        self,
        url: str,
        api_token: Optional[str] = None,
        timeout: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        Constructor

        Args:
            url: URL to connect to the storage
            api_token: API token if the storage uses it for authorization
            timeout: total timeout for connection, request and response in seconds
            extra_headers: extra headers to send with each request
        Kwargs:
            session: an external aiohttp session to use for requests
            verify_ssl: verify SSL certificates
        Examples:
            >>> client = Client("http://127.0.0.1:8383")
            >>> info = await client.info()
        """
        self._http = HttpClient(
            url.rstrip("/"), api_token, timeout, extra_headers, **kwargs
        )

    async def __aenter__(self):
        self._http._session = ClientSession(timeout=self._http._timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._http._session.close()

    async def info(self) -> ServerInfo:
        """
        Get high level server info

        Returns:
            ServerInfo: server information

        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/info")
        return ServerInfo.model_validate_json(body)

    async def list(self) -> List[BucketInfo]:
        """
        Get a list of all buckets on server

        Returns:
            List[BucketInfo]: the list of buckets
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/list")
        return BucketList.model_validate_json(body).buckets

    async def get_bucket(self, name: str) -> Bucket:
        """
        Load a bucket to work with
        Args:
            name: name of the bucket
        Returns:
            Bucket: the bucket object
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
        data = settings.model_dump_json() if settings else None
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
            List[Token]: the list of tokens
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/tokens")
        return TokenList.model_validate_json(body).tokens

    async def get_token(self, name: str) -> FullTokenInfo:
        """
        Get a token by name
        Args:
            name: name of the token
        Returns:
            FullTokenInfo: the token information with permissions
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", f"/tokens/{name}")
        return FullTokenInfo.model_validate_json(body)

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
        body, _ = await self._http.request_all(
            "POST", f"/tokens/{name}", data=permissions.model_dump_json()
        )
        return TokenCreateResponse.model_validate_json(body).value

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
            FullTokenInfo: the current token information with permission
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/me")
        return FullTokenInfo.model_validate_json(body)

    async def get_replications(self) -> List[ReplicationInfo]:
        """
        Get a list of replications
        Returns:
            List[ReplicationInfo]: List of replications with their statuses
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/replications")
        return ReplicationList.model_validate_json(body).replications

    async def get_replication_detail(
        self, replication_name: str
    ) -> ReplicationDetailInfo:
        """
        Get detailed information about a replication
        Args:
            replication_name: Name of the replication to show details
        Returns:
            ReplicationDetailInfo: Detailed information about the replication
        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all(
            "GET", f"/replications/{replication_name}"
        )
        return ReplicationDetailInfo.model_validate_json(body)

    async def create_replication(
        self, replication_name: str, settings: ReplicationSettings
    ) -> None:
        """
        Create a new replication
        Args:
            replication_name: Name of the new replication
            settings: Settings for the new replication
        Raises:
            ReductError: if there is an HTTP error
        """
        data = settings.model_dump_json()
        await self._http.request_all(
            "POST", f"/replications/{replication_name}", data=data
        )

    async def update_replication(
        self, replication_name: str, settings: ReplicationSettings
    ) -> None:
        """
        Update an existing replication
        Args:
            replication_name: Name of the replication to update
            settings: New settings for the replication
        Raises:
            ReductError: if there is an HTTP error
        """
        data = settings.model_dump_json()
        await self._http.request_all(
            "PUT", f"/replications/{replication_name}", data=data
        )

    async def delete_replication(self, replication_name: str) -> None:
        """
        Delete a replication
        Args:
            replication_name: Name of the replication to delete
        Raises:
            ReductError: if there is an HTTP error
        """
        await self._http.request_all("DELETE", f"/replications/{replication_name}")
