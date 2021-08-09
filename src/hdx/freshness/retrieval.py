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

ignore_mimetypes = ['application/octet-stream', 'application/binary']
mimetypes = {'json': ['application/json'], 'geojson': ['application/json', 'application/geo+json'],
             'shp': ['application/zip', 'application/x-zip-compressed'],
             'csv': ['text/csv', 'application/zip', 'application/x-zip-compressed'],
             'xls': ['application/vnd.ms-excel'],
             'xlsx': ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet']}
signatures = {'json': [b'[', b' [', b'{', b' {'], 'geojson': [b'[', b' [', b'{', b' {'], 'shp': [b'PK\x03\x04'],
              'xls': [b'\xd0\xcf\x11\xe0'], 'xlsx': [b'PK\x03\x04']}


async def fetch(metadata, session):
    url = metadata[0]
    resource_id = metadata[1]
    resource_format = metadata[2]

    async def fn(response):
        last_modified_str = response.headers.get('Last-Modified')
        http_last_modified = None
        if last_modified_str:
            try:
                http_last_modified = parser.parse(last_modified_str, ignoretz=True)
                # we set this but don't actually use it to calculate freshness any more
            except (ValueError, OverflowError):
                pass
        length = response.headers.get('Content-Length')
        if length and int(length) > 419430400:
            response.close()
            err = 'File too large to hash!'
            return resource_id, url, resource_format, err, http_last_modified, None
        logger.info('Hashing %s' % url)
        mimetype = response.headers.get('Content-Type')
        signature = None

        try:
            md5hash = hashlib.md5()
            async for chunk in response.content.iter_chunked(10240):
                if chunk:
                    md5hash.update(chunk)
                    if not signature:
                        signature = chunk[:4]
            err = None
            if mimetype not in ignore_mimetypes:
                expected_mimetypes = mimetypes.get(resource_format)
                if expected_mimetypes is not None:
                    if not any(x in mimetype for x in expected_mimetypes):
                        err = 'File mimetype %s does not match HDX format %s!' % (mimetype, resource_format)
            expected_signatures = signatures.get(resource_format)
            if expected_signatures is not None:
                found = False
                for expected_signature in expected_signatures:
                    if signature[:len(expected_signature)] == expected_signature:
                        found = True
                        break
                if not found:
                    sigerr = 'File signature %s does not match HDX format %s!' % (signature, resource_format)
                    if err is None:
                        err = sigerr
                    else:
                        err = f'{err} {sigerr}'
            return resource_id, url, resource_format, err, http_last_modified, md5hash.hexdigest()
        except Exception as exc:
            try:
                code = exc.code
            except AttributeError:
                code = ''
            err = 'Exception during hashing: code=%s message=%s raised=%s.%s url=%s' % (code, exc,
                                                                                         exc.__class__.__module__,
                                                                                         exc.__class__.__qualname__,
                                                                                         url)
            raise aiohttp.ClientResponseError(code=code, message=err,
                                              request_info=response.request_info, history=response.history) from exc

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=2,
                                     interval=5,
                                     backoff=4,
                                     fn=fn)
    except Exception as e:
        return resource_id, url, resource_format, str(e), None, None


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
            resource_id, url, resource_format, err, http_last_modified, hash = await f
            responses[resource_id] = (url, resource_format, err, http_last_modified, hash)
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
