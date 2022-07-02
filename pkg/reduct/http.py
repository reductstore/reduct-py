"""Internal HTTP helper"""
import hashlib
import json
from typing import Optional, AsyncIterator

import aiohttp
from aiohttp import ClientTimeout

from reduct.error import ReductError


# pylint: disable=too-few-public-methods
class HttpClient:
    """Wrapper for HTTP calls"""

    def __init__(
        self, url: str, api_token: Optional[str] = None, timeout: Optional[float] = None
    ):
        self.url = url
        self.api_token = api_token
        self.headers = {}
        self.timeout = ClientTimeout(timeout) if timeout else ClientTimeout()

    async def request_by(
        self, method: str, path: str = "", chunk_size=1024, **kwargs
    ) -> AsyncIterator[bytes]:
        """HTTP request with ReductError exception by chunks"""

        extra_headers = {}
        if "content_length" in kwargs:
            extra_headers["Content-Length"] = str(kwargs["content_length"])
            del kwargs["content_length"]

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            while True:  # We need cycle to repeat request if the token expires
                async with session.request(
                    method,
                    f"{self.url}{path.strip()}",
                    headers=dict(self.headers, **extra_headers),
                    **kwargs,
                ) as response:

                    if response.ok:
                        async for chunk in response.content.iter_chunked(chunk_size):
                            yield chunk
                        return  # Success

                    if response.status == 401:
                        # Authentication issue, try to refresh token and repeat request
                        hasher = hashlib.sha256(bytes(self.api_token, "utf-8"))
                        async with session.post(
                            f"{self.url}/auth/refresh",
                            headers={"Authorization": f"Bearer {hasher.hexdigest()}"},
                        ) as auth_resp:
                            if auth_resp.status == 200:
                                data = json.loads(await auth_resp.read())
                                self.headers = {
                                    "Authorization": f'Bearer {data["access_token"]}'
                                }
                                continue

                    raise ReductError(response.status, await response.text())

    async def request(self, method: str, path: str = "", **kwargs) -> bytes:
        """Http request"""
        blob = b""
        async for chunk in self.request_by(method, path, chunk_size=1024, **kwargs):
            blob += chunk
        return blob
