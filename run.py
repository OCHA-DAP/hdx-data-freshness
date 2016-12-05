#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import logging
import datetime

from urllib.parse import urlparse

import hashlib

import tqdm
from dateutil import parser
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from hdx.data.resource import Resource
from hdx.facades.simple import facade

import retry
from database.base import Base
from database.dbresource import DBResource

logger = logging.getLogger(__name__)
engine = create_engine('sqlite:///resources.db', echo=False)
Session = sessionmaker(bind=engine)
md5hash = hashlib.md5()


import asyncio
import functools
import time

import aiohttp


def throttling(sleep):
    def decorator(func):
        last_called = None
        lock = asyncio.Lock()

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal last_called

            async with lock:
                now = time.monotonic()

                if last_called is not None:
                    delta = now - last_called

                    if delta < sleep:
                        await asyncio.sleep(sleep - delta)

                try:
                    return await func(*args, **kwargs)
                finally:
                    last_called = time.monotonic()

        return wrapper

    return decorator

async def fetch(metadata, session):
    url, resource_id = metadata
    async def fn(response):
        last_modified = response.headers.get('Last-Modified', None)
        if last_modified:
            response.close()
            return resource_id, url, 1, last_modified
        logger.info('Hashing %s' % url)
        async for chunk in response.content.iter_chunked(1024):
            if chunk:
                md5hash.update(chunk)
        return resource_id, url, 2, md5hash.hexdigest()

    try:
        return await retry.send_http(session, 'get', url,
                                     retries=5,
                                     interval=0.4,
                                     backoff=2,
                                     connect_timeout=10,
                                     read_timeout=300,
                                     http_status_codes_to_retry=[429, 500, 502, 503, 504],
                                     fn=fn)
    except Exception as e:
        return resource_id, url, 0, str(e)


_domain_funcq_map = {}

async def delayed_fetch(metadata, session):
    domain = urlparse(metadata[0]).hostname
    now = time.monotonic()
    if domain not in _domain_funcq_map:
        _domain_funcq_map[domain] = now
    else:
        delta = now - _domain_funcq_map[domain]
        if delta < 10:
            await asyncio.sleep(10 - delta)
    data = await fetch(metadata, session)
    _domain_funcq_map[domain] = time.monotonic()
    return data


async def bound_fetch(sem, metadata, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(metadata, session)


async def check_resources_for_last_modified(last_modified_check, loop):
    tasks = list()

    # create instance of Semaphore
    sem = asyncio.Semaphore(100)

    conn = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=conn, loop=loop) as session:
        for metadata in last_modified_check:
            task = asyncio.ensure_future(bound_fetch(sem, metadata, session))
            tasks.append(task)
        responses = []
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            responses.append(await f)
        return responses


def set_last_modified(dbresource, modified_date):
    dbresource.http_last_modified = parser.parse(modified_date, ignoretz=True)
    if dbresource.last_modified:
        if dbresource.http_last_modified > dbresource.last_modified:
            dbresource.last_modified = dbresource.http_last_modified
    else:
        dbresource.last_modified = dbresource.http_last_modified


def main(configuration):
    ''''''
    Base.metadata.create_all(engine)
    session = Session()
    resources = Resource.search_in_hdx(configuration, 'name:')
    total = len(resources)
    datahumdataorg_count = 0
    managehdxrwlabsorg_count = 0
    proxyhxlstandardorg_count = 0
    scraperwikicom_count = 0
    ourairportscom_count = 0
    last_modified_check = list()
    for resource in resources:
        resource_id = resource['id']
        url = resource['url']
        name = resource['name']
        revision_last_updated = resource.get('revision_last_updated', None)
        if revision_last_updated:
            revision_last_updated = parser.parse(revision_last_updated, ignoretz=True)
        dbresource = session.query(DBResource).filter_by(id=resource_id).first()
        if dbresource is None:
            dbresource = DBResource(id=resource_id, name=name, url=url,
                                    last_modified=revision_last_updated, revision_last_updated=revision_last_updated)
            session.add(dbresource)
        else:
            dbresource.name = name
            dbresource.url = url
            dbresource.last_modified = revision_last_updated
            dbresource.revision_last_updated = revision_last_updated
        if 'data.humdata.org' in url:
            datahumdataorg_count += 1
            continue
        if 'manage.hdx.rwlabs.org' in url:
            managehdxrwlabsorg_count += 1
            continue
        if 'proxy.hxlstandard.org' in url:
            proxyhxlstandardorg_count += 1
            continue
        if 'scraperwiki.com' in url:
            scraperwikicom_count += 1
            continue
        if 'ourairports.com' in url:
            ourairportscom_count += 1
            continue
        last_modified_check.append((url, resource_id))
    session.commit()
    start_time = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(check_resources_for_last_modified(last_modified_check, loop))
    results = loop.run_until_complete(future)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    lastmodified_count = 0
    hash_updated_count = 0
    hash_unchanged_count = 0
    failed_count = 0
    for resource_id, url, status, result in results:
        dbresource = session.query(DBResource).filter_by(id=resource_id).first()
        if status == 0:
            failed_count += 1
            dbresource.error = result
        elif status == 1:
            lastmodified_count += 1
            set_last_modified(dbresource, result)
        elif status == 2:
            if dbresource.md5_hash == result:  # File unchanged
                hash_unchanged_count += 1
            else:  # File updated
                dbresource.md5_hash = result
                dbresource.last_hash_date = datetime.datetime.utcnow()
                hash_updated_count += 1
        else:
            raise ValueError('Invalid status returned!')
    session.commit()
    str = '\ndata.humdata.org: %d, manage.hdx.rwlabs.org: %d, ' % (datahumdataorg_count, managehdxrwlabsorg_count)
    str += 'proxy.hxlstandard.org: %d, scraperwiki.com: %d, ' % (proxyhxlstandardorg_count, scraperwikicom_count)
    str += 'ourairports.com: %d\n' % ourairportscom_count
    str += 'Have Last-Modified: %d, Hash updated: %d, ' % (lastmodified_count, hash_updated_count)
    str += 'Hash Unchanged: %d\n' % hash_unchanged_count
    str += 'Number Failed: %d, Total number: %d' % (failed_count, total)
    logger.info(str)

if __name__ == '__main__':
    facade(main, hdx_site='prod')
