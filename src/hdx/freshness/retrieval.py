#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Retrieval
---------

Retrieve urls and categorise them

'''
import asyncio
import hashlib
import logging
from timeit import default_timer as timer

import aiohttp
import tqdm
import uvloop
from dateutil import parser

from hdx.freshness import retry
from hdx.freshness.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)


async def fetch(metadata, session):
    url = metadata[0]
    resource_id = metadata[1]
    force_hash = metadata[2]

    async def fn(response):
        last_modified_str = response.headers.get('Last-Modified')
        http_last_modified = None
        if last_modified_str:
            try:
                http_last_modified = parser.parse(last_modified_str, ignoretz=True)
                if not force_hash:
                    response.close()
                    return resource_id, url, None, http_last_modified, None, False
            except (ValueError, OverflowError):
                pass
        length = response.headers.get('Content-Length')
        if length and int(length) > 419430400:
            response.close()
            err = 'File too large to hash!'
            return resource_id, url, err, http_last_modified, None, force_hash
        logger.info('Hashing %s' % url)
        try:
            md5hash = hashlib.md5()
            async for chunk in response.content.iter_chunked(10240):
                if chunk:
                    md5hash.update(chunk)
            return resource_id, url, None, http_last_modified, md5hash.hexdigest(), force_hash
        except Exception as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ''
            err = 'Exception during hashing: code=%s message=%s raised=%s.%s url=%s' % (code, exc,
                                                                                         exc.__class__.__module__,
                                                                                         exc.__class__.__qualname__,
                                                                                         url)
            if http_last_modified:
                return resource_id, url, err, http_last_modified, None, force_hash
            raise aiohttp.ClientResponseError(code=code, message=err,
                                              request_info=response.request_info, history=response.history) from exc

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=2,
                                     interval=5,
                                     backoff=4,
                                     fn=fn)
    except Exception as e:
        return resource_id, url, str(e), None, None, force_hash


async def check_urls(urls, loop, user_agent):
    tasks = list()

    conn = aiohttp.TCPConnector(limit=100, limit_per_host=1, loop=loop)
    timeout = aiohttp.ClientTimeout(total=60 * 60, sock_connect=30, sock_read=30)
    async with aiohttp.ClientSession(connector=conn, timeout=timeout, loop=loop,
                                     headers={'User-Agent': user_agent}) as session:
        session = RateLimiter(session)
        for metadata in urls:
            task = fetch(metadata, session)
            tasks.append(task)
        responses = dict()
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            resource_id, url, err, http_last_modified, hash, force_hash = await f
            responses[resource_id] = (url, err, http_last_modified, hash, force_hash)
        return responses


def retrieve(urls, user_agent):
    start_time = timer()
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(check_urls(urls, loop, user_agent))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (timer() - start_time))
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()
    return results
