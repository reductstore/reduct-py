"""Internal HTTP helper"""
from contextlib import asynccontextmanager
from typing import Optional, AsyncIterator

import aiohttp
from aiohttp import ClientTimeout, ClientResponse

from reduct.error import ReductError


class HttpClient:
    """Wrapper for HTTP calls"""

    def __init__(
        self, url: str, api_token: Optional[str] = None, timeout: Optional[float] = None
    ):
        self.url = url
        self.api_token = api_token
        self.headers = (
            {"Authorization": f"Bearer {api_token}"} if api_token is not None else {}
        )
        self.timeout = ClientTimeout(timeout)

    @asynccontextmanager
    async def request(self, method: str, path: str = "", **kwargs) -> ClientResponse:
        """HTTP request with ReductError exception"""

        extra_headers = {}
        if "content_length" in kwargs:
            extra_headers["Content-Length"] = str(kwargs["content_length"])
            del kwargs["content_length"]

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.request(
                method,
                f"{self.url}{path.strip()}",
                headers=dict(self.headers, **extra_headers),
                **kwargs,
            ) as response:

                if response.ok:
                    yield response

                else:
                    raise ReductError(response.status, await response.text())

    async def request_all(self, method: str, path: str = "", **kwargs) -> bytes:
        """Http request"""
        async with self.request(method, path, **kwargs) as response:
            return await response.read()

    async def request_by(
        self, method: str, path: str = "", chunk_size=1024, **kwargs
    ) -> AsyncIterator[bytes]:
        """Http request"""
        async with self.request(method, path, **kwargs) as response:
            async for chunk in response.content.iter_chunked(chunk_size):
                yield chunk
        return
