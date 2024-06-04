"""Main client code"""

from datetime import datetime
from typing import Optional, List, Dict

from aiohttp import ClientSession
from pydantic import BaseModel

from reduct.bucket import BucketInfo, BucketSettings, Bucket
from reduct.http import HttpClient
from reduct.error import ReductError


class Defaults(BaseModel):
    """Default server settings"""

    bucket: BucketSettings
    """settings for a new bucket"""


class LicenseInfo(BaseModel):
    """License information"""

    licensee: str
    """Licensee usually is the company name"""

    invoice: str
    """Invoice number"""

    expiry_date: datetime
    """Expiry date of the license in ISO 8601 format (UTC)"""

    plan: str
    """Plan name"""

    device_number: int
    """Number of devices (0 for unlimited)"""

    disk_quota: int
    """Disk quota in TB (0 for unlimited)"""

    fingerprint: str
    """License fingerprint"""


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

    license: Optional[LicenseInfo] = None
    """license information"""

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

    is_provisioned: bool = False
    """token is provisioned and can't be deleted or changed"""


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


class ReplicationInfo(BaseModel):
    """Replication information"""

    name: str
    """name of the replication"""
    is_provisioned: bool
    """replication is provisioned and can't be deleted or changed"""
    is_active: bool
    """replication is active and the remote server is reachable"""
    pending_records: int
    """number of records to replicate"""


class ReplicationList(BaseModel):
    """List of replications"""

    replications: List[ReplicationInfo]


class ReplicationDiagnosticsError(BaseModel):
    """Error information for replication"""

    count: int
    last_message: str


class ReplicationDiagnosticsDetail(BaseModel):
    """Diagnostics information for replication"""

    ok: int
    """number of successful replications"""
    errored: int
    """number of failed replications"""
    errors: Dict[int, ReplicationDiagnosticsError]
    """list of errors grouped by status code"""


class ReplicationDiagnostics(BaseModel):
    """Detailed diagnostics for replication"""

    hourly: ReplicationDiagnosticsDetail
    """hourly diagnostics"""


class ReplicationSettings(BaseModel):
    """Settings for creating a replication"""

    src_bucket: str
    """source bucket name"""
    dst_bucket: str
    """destination bucket name"""
    dst_host: str
    """url of the destination instance"""
    dst_token: str = ""
    """access token for the destination instance"""
    entries: List[str] = []
    """list of entries to replicate. If empty, all entries are replicated.
    Wildcards are supported"""
    include: Dict[str, str] = {}
    """replicate only records with these labels"""
    exclude: Dict[str, str] = {}
    """exclude records with these labels"""
    each_s: Optional[float] = None
    """replicate a record every S seconds"""
    each_n: Optional[int] = None
    """replicate every Nth record"""


class ReplicationDetailInfo(BaseModel):
    """Complete information about a replication"""

    diagnostics: ReplicationDiagnostics
    """diagnostics information"""
    info: ReplicationInfo
    """replication information"""
    settings: ReplicationSettings
    """replication settings"""


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
            ServerInfo:

        Raises:
            ReductError: if there is an HTTP error
        """
        body, _ = await self._http.request_all("GET", "/info")
        return ServerInfo.model_validate_json(body)

    async def list(self) -> List[BucketInfo]:
        """
        Return a list of all buckets on server

        Returns:
            List[BucketInfo]
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
            List[Token]
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
            Token
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
            FullTokenInfo
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
