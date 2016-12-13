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

import retry

logger = logging.getLogger(__name__)


async def fetch(metadata, session):
    url, resource_id = metadata
    async def fn(response):
        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            response.close()
            return resource_id, url, 1, last_modified
        length = response.headers.get('Content-Length')
        if length and int(length) > 419430400:
            response.close()
            return resource_id, url, 0, 'File too large to hash!'
        logger.info('Hashing %s' % url)
        try:
            md5hash = hashlib.md5()
            async for chunk in response.content.iter_chunked(10240):
                if chunk:
                    md5hash.update(chunk)
            return resource_id, url, 2, md5hash.hexdigest()
        except Exception as e:
            raise type(e)('%s during hashing' % str(e)).with_traceback(sys.exc_info()[2])

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=5,
                                     interval=0.4,
                                     backoff=2,
                                     read_timeout=300,
                                     http_status_codes_to_retry=[429, 500, 502, 503, 504],
                                     fn=fn)
    except Exception as e:
        return resource_id, url, 0, str(e)

async def bound_fetch(sem, metadata, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(metadata, session)

async def check_resources_for_last_modified(last_modified_check, loop):
    tasks = list()

    # create instance of Semaphore
    sem = asyncio.Semaphore(100)

    conn = aiohttp.TCPConnector(conn_timeout=10, limit=2)
    async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
        for metadata in last_modified_check:
            task = bound_fetch(sem, metadata, session)
            tasks.append(task)
        responses = dict()
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            resource_id, url, status, result = await f
            responses[resource_id] = (url, status, result)
        return responses


def retrieve(metadata):
    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(check_resources_for_last_modified(metadata, loop))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    return results
