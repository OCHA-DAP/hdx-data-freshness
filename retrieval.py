#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Retrieval
---------

Retrieve urls and categorise them

'''
import sys
import logging
import time
import asyncio
import aiohttp
import hashlib

import tqdm
from dateutil import parser

import retry

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
            raise type(e)(err).with_traceback(sys.exc_info()[2])

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=5,
                                     interval=0.4,
                                     backoff=2,
                                     read_timeout=300,
                                     http_status_codes_to_retry=[429, 500, 502, 503, 504],
                                     fn=fn)
    except Exception as e:
        return resource_id, url, str(e), None, None, force_hash

async def bound_fetch(sem, metadata, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(metadata, session)

async def check_urls(urls, loop):
    tasks = list()

    # create instance of Semaphore
    sem = asyncio.Semaphore(100)

    conn = aiohttp.TCPConnector(conn_timeout=10, limit=2)
    async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
        for metadata in urls:
            task = bound_fetch(sem, metadata, session)
            tasks.append(task)
        responses = dict()
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            resource_id, url, err, http_last_modified, hash, force_hash = await f
            responses[resource_id] = (url, err, http_last_modified, hash, force_hash)
        return responses


def retrieve(urls):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(check_urls(urls, loop))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    return results
