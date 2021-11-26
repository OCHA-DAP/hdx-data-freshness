"""aiohttp rate limiting: limit connections per timeframe to host
(from https://quentin.pradet.me/blog/how-do-you-rate-limit-calls-with-aiohttp.html)
"""
import asyncio
import time
from typing import Any
from urllib.parse import urlsplit

from aiohttp.client import _RequestContextManager


class RateLimiter:
    """
    Use like this:
    session = RateLimiter(session)
    ...
    async with await session.get(url) as response

    Args:
        session (aiohttp.ClientSession): aiohttp session to use for requests
    """

    RATE = 9 / 60  # requests per second
    MAX_TOKENS = 10

    def __init__(self, session):
        self.session = session
        self.start_time = time.monotonic()
        self.tokens = dict()

    async def get(
        self, url: str, *args: Any, **kwargs: Any
    ) -> _RequestContextManager:
        """Asynchronous code to download a resource after waiting for a token

        Args:
            url (str): Url to download
            *args
            **kwargs

        Returns:
            aiohttp.ClientResponse: Response from request
        """
        host = urlsplit(url).netloc
        await self.wait_for_token(host)
        return self.session.get(url, *args, **kwargs)

    async def wait_for_token(self, host: str) -> None:
        """Asynchronous code to handle sleeping if host already connected to

        Args:
            host (str): Host (server)

        Returns:
            None
        """
        if host not in self.tokens:
            self.tokens[host] = [self.MAX_TOKENS, self.start_time]
        while self.tokens[host][0] < 1:
            self.add_new_tokens(host)
            await asyncio.sleep(1)
        self.tokens[host][0] -= 1

    def add_new_tokens(self, host: str) -> None:
        """Adds new tokens

        Args:
            host (str): Host (server)

        Returns:
            None
        """
        now = time.monotonic()
        time_since_update = now - self.tokens[host][1]
        new_tokens = time_since_update * self.RATE
        if new_tokens > 1:
            self.tokens[host][0] = min(
                self.tokens[host][0] + new_tokens, self.MAX_TOKENS
            )
            self.tokens[host][1] = now
