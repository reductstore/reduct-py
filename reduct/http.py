"""Internal HTTP helper"""

from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator, Dict, Tuple

import aiohttp
from aiohttp import ClientTimeout, ClientResponse
from aiohttp.client_exceptions import ClientConnectorError

from reduct.error import ReductError

API_PREFIX = "/api/v1"


class HttpClient:
    """Wrapper for HTTP calls"""

    FILE_SIZE_FOR_100_CONTINUE = 64_000_000

    def __init__(
        self,
        url: str,
        api_token: Optional[str] = None,
        timeout: Optional[float] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        self._url = url + API_PREFIX
        self._api_token = api_token
        self._headers = (
            {"Authorization": f"Bearer {api_token}"} if api_token is not None else {}
        )
        if extra_headers:
            self._headers.update(extra_headers)

        self._timeout = ClientTimeout(timeout)
        self._api_version = None
        self._session = kwargs.pop("session", None)
        self._verify_ssl = kwargs.pop("verify_ssl", True)

    @asynccontextmanager
    async def request(  # pylint: disable=contextmanager-generator-missing-cleanup
        self, method: str, path: str = "", **kwargs
    ) -> AsyncIterator[ClientResponse]:
        """HTTP request with ReductError exception"""

        extra_headers = kwargs.pop("extra_headers", {})
        expect100 = False

        if "content_length" in kwargs:
            content_length = kwargs["content_length"]
            extra_headers["Content-Length"] = str(content_length)
            if content_length > self.FILE_SIZE_FOR_100_CONTINUE:
                # Use 100-continue for large files
                expect100 = True

            del kwargs["content_length"]

        if "content_type" in kwargs:
            extra_headers["Content-Type"] = str(kwargs["content_type"])
            del kwargs["content_type"]

        if "labels" in kwargs:
            if kwargs["labels"]:
                for name, value in kwargs["labels"].items():
                    extra_headers[f"x-reduct-label-{name}"] = str(value)
            del kwargs["labels"]

        kwargs["verify_ssl"] = self._verify_ssl

        if self._session is None:
            connector = aiohttp.TCPConnector(force_close=True)
            async with aiohttp.ClientSession(
                timeout=self._timeout, connector=connector
            ) as session:
                async with self._request(
                    method, path, session, extra_headers, expect100=expect100, **kwargs
                ) as response:
                    yield response
        else:
            async with self._request(
                method,
                path,
                self._session,
                extra_headers,
                expect100=expect100,
                **kwargs,
            ) as response:
                yield response

    @asynccontextmanager
    async def _request(
        self, method, path, session, extra_headers, **kwargs
    ) -> AsyncIterator[ClientResponse]:
        try:
            async with session.request(
                method,
                f"{self._url}{path.strip()}",
                headers=dict(self._headers, **extra_headers),
                **kwargs,
            ) as response:
                if self._api_version is None:
                    self._api_version = response.headers.get("x-reduct-api")

                if response.ok:
                    yield response
                else:
                    if "x-reduct-error" in response.headers:
                        raise ReductError(
                            response.status,
                            response.headers["x-reduct-error"],
                        )
                    raise ReductError(response.status, "Unknown error")
        except ClientConnectorError as error:
            raise ReductError(599, str(error)) from None

    async def request_all(
        self, method: str, path: str = "", **kwargs
    ) -> (bytes, Dict[str, str]):
        """Http request
        Args:
            method (str): HTTP method
            path (str, optional): Path. Defaults to "".
            **kwargs: kwargs for aiohttp.request
        Kwargs:
            data (bytes | AsyncIterator[bytes): request body
            extra_headers (Dict[str, str]): extra headers
            content_length (int): content length
        Returns:
            bytes: response body
            Dict[str, str]: response headers
        Raises:
            ReductError: if request failed
        """
        async with self.request(method, path, **kwargs) as response:
            return await response.read(), response.headers

    async def request_chunked(  # pylint: disable=contextmanager-generator-missing-cleanup
        self, method: str, path: str = "", chunk_size=1024, **kwargs
    ) -> AsyncIterator[bytes]:
        """Http request"""
        async with self.request(method, path, **kwargs) as response:
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk
        return

    @property
    def api_version(self) -> Optional[Tuple[int, int]]:
        """API version"""
        if self._api_version is None:
            return None
        return extract_api_version(self._api_version)


def extract_api_version(version: str) -> Tuple[int, int]:
    """Extract version"""
    major, minor = version.split(".")
    return int(major), int(minor)
