#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Rate limit
----------

Limit connections per timeframe to host

'''
import asyncio
import time
from urllib.parse import urlsplit


class RateLimiter:
    RATE = 9 / 60
    MAX_TOKENS = 10

    def __init__(self, session):
        self.session = session
        self.start_time = time.monotonic()
        self.tokens = dict()

    async def get(self, url, *args, **kwargs):
        host = urlsplit(url).netloc
        await self.wait_for_token(host)
        return self.session.get(url, *args, **kwargs)

    async def wait_for_token(self, host):
        if host not in self.tokens:
            self.tokens[host] = [self.MAX_TOKENS, self.start_time]
        while self.tokens[host][0] < 1:
            self.add_new_tokens(host)
            await asyncio.sleep(1)
        self.tokens[host][0] -= 1

    def add_new_tokens(self, host):
        now = time.monotonic()
        time_since_update = now - self.tokens[host][1]
        new_tokens = time_since_update * self.RATE
        if new_tokens > 1:
            self.tokens[host][0] = min(self.tokens[host][0] + new_tokens, self.MAX_TOKENS)
            self.tokens[host][1] = now
