"""Internal HTTP helper"""
import hashlib
import json
from typing import Optional

import aiohttp

from reduct.error import ReductError


# pylint: disable=too-few-public-methods
class HttpClient:
    """Wrapper for HTTP calls"""

    def __init__(self, url: str, api_token: Optional[str] = None):
        self.url = url
        self.api_token = api_token
        self.headers = {}

    async def request(self, method: str, path: str = "", **kwargs) -> bytes:
        """HTTP request with ReductError exception"""
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, f"{self.url}{path.strip()}", headers=self.headers, **kwargs
            ) as response:
                if response.ok:
                    return await response.read()

                if response.status == 401:
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
                            return await self.request(method, path, **kwargs)

                raise ReductError(response.status, await response.text())
