"""Internal HTTP helper"""
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator

import aiohttp
from aiohttp import ClientTimeout, ClientResponse
from aiohttp.client_exceptions import ClientConnectorError

from reduct.error import ReductError

API_PREFIX = "/api/v1"


class HttpClient:
    """Wrapper for HTTP calls"""

    def __init__(
        self, url: str, api_token: Optional[str] = None, timeout: Optional[float] = None
    ):
        self.url = url + API_PREFIX
        self.api_token = api_token
        self.headers = (
            {"Authorization": f"Bearer {api_token}"} if api_token is not None else {}
        )
        self.timeout = ClientTimeout(timeout)

    @asynccontextmanager
    async def request(
        self, method: str, path: str = "", **kwargs
    ) -> AsyncIterator[ClientResponse]:
        """HTTP request with ReductError exception"""

        extra_headers = {}
        if "content_length" in kwargs:
            extra_headers["Content-Length"] = str(kwargs["content_length"])
            del kwargs["content_length"]

        if "content_type" in kwargs:
            extra_headers["Content-Type"] = str(kwargs["content_type"])
            del kwargs["content_type"]

        if "labels" in kwargs:
            if kwargs["labels"]:
                for name, value in kwargs["labels"].items():
                    extra_headers[f"x-reduct-label-{name}"] = str(value)
            del kwargs["labels"]

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            try:
                async with session.request(
                    method,
                    f"{self.url}{path.strip()}",
                    headers=dict(self.headers, **extra_headers),
                    **kwargs,
                ) as response:

                    if response.ok:
                        yield response
                    else:
                        if "x-reduct-error" in response.headers:
                            raise ReductError(
                                response.status,
                                response.headers["x-reduct-error"],
                            )
                        raise ReductError(response.status, "Unknown error")
            except ClientConnectorError:
                raise ReductError(
                    599, f"Connection failed, server {self.url} cannot be reached"
                ) from None

    async def request_all(self, method: str, path: str = "", **kwargs) -> bytes:
        """Http request"""
        async with self.request(method, path, **kwargs) as response:
            return await response.read()

    async def request_chunked(
        self, method: str, path: str = "", chunk_size=1024, **kwargs
    ) -> AsyncIterator[bytes]:
        """Http request"""
        async with self.request(method, path, **kwargs) as response:
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk
        return
