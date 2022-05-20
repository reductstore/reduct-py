import aiohttp

from reduct.error import ReductError


async def request(method: str, url: str, **kwargs) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, **kwargs) as response:
            if response.ok:
                return await response.read()
            raise ReductError(response.status, await response.text())
