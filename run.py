#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import sys
import logging
import datetime
import time
import asyncio
import aiohttp
import hashlib

import tqdm
from dateutil import parser
from hdx.data.dataset import Dataset
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from hdx.facades.simple import facade

import retry
from database.base import Base
from database.dbdataset import DBDataset
from database.dbresource import DBResource

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
        responses = []
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            responses.append(await f)
        return responses


def set_last_modified(dbresource, modified_date, updated):
    if dbresource.last_modified:
        if modified_date > dbresource.last_modified:
            dbresource.last_modified = modified_date
            dbresource.updated = updated
    else:
        dbresource.last_modified = modified_date
        dbresource.updated = updated


def main(configuration):
    ''''''
    engine = create_engine('sqlite:///datasets.db', echo=False)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()
    datasets = Dataset.get_all_datasets(configuration, False)
    total_datasets = len(datasets)
    total = 0
    still_fresh_count = 0
    datahumdataorg_count = 0
    managehdxrwlabsorg_count = 0
    proxyhxlstandardorg_count = 0
    scraperwikicom_count = 0
    ourairportscom_count = 0
    revision_count = 0
    last_modified_check = list()
    for dataset in datasets:
        dataset_id = dataset['id']
        dbdataset = session.query(DBDataset).filter_by(id=dataset_id).first()
        resources = dataset.get_resources()
        fresh = None
        fresh_days = None
        update_frequency = dataset.get('data_update_frequency')
        if update_frequency is not None:
            update_frequency = int(update_frequency)
            if update_frequency == 0:
                fresh = True
            else:
                fresh_days = datetime.timedelta(days=update_frequency)
                if dbdataset:
                    fresh_end = dbdataset.last_modified + fresh_days
                    if fresh_end >= datetime.datetime.utcnow():
                        still_fresh_count += len(resources)
                        for dbresource in session.query(DBResource).filter_by(dataset_id=dataset_id):
                            dbresource.updated = ''
                        continue
                    fresh = False
        dataset_last_modified = None
        resource_updated = None
        total += len(resources)
        dataset_resources = list()
        for resource in resources:
            resource_id = resource['id']
            url = resource['url']
            name = resource['name']
            revision_last_updated = resource.get('revision_last_updated', None)
            if revision_last_updated:
                revision_last_updated = parser.parse(revision_last_updated, ignoretz=True)
            if dataset_last_modified:
                if revision_last_updated > dataset_last_modified:
                    dataset_last_modified = revision_last_updated
                    resource_updated = resource_id
            else:
                dataset_last_modified = revision_last_updated
                resource_updated = resource_id
            dbresource = session.query(DBResource).filter_by(id=resource_id).first()
            if dbresource is None:
                dbresource = DBResource(id=resource_id, name=name, dataset_id=dataset_id, url=url,
                                        last_modified=revision_last_updated, revision_last_updated=revision_last_updated,
                                        updated='revision')
                session.add(dbresource)
            else:
                dbresource.name = name
                dbresource.url = url
                dbresource.updated = ''
                if revision_last_updated > dbresource.revision_last_updated:
                    dbresource.revision_last_updated = revision_last_updated
                if revision_last_updated > dbresource.last_modified:
                    dbresource.last_modified = revision_last_updated
                    dbresource.updated = 'revision'
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
            dataset_resources.append((url, resource_id))
        dataset_name = dataset['name']
        dataset_date = dataset.get('dataset_date')
        if fresh_days is not None:
            fresh_end = dataset_last_modified + fresh_days
            if fresh_end >= datetime.datetime.utcnow():
                fresh = True
            else:
                fresh = False
        if dbdataset is None:
            dbdataset = DBDataset(id=dataset_id, name=dataset_name, dataset_date=dataset_date,
                                  update_frequency=update_frequency, last_modified=dataset_last_modified,
                                  resource_updated=resource_updated, fresh=fresh, error=False)
            session.add(dbdataset)
        else:
            dbdataset.name = dataset_name
            dbdataset.dataset_date = dataset_date
            dbdataset.update_frequency = update_frequency
            dbdataset.resource_updated = ''
            if dataset_last_modified > dbdataset.last_modified:
                dbdataset.last_modified = dataset_last_modified
                dbdataset.resource_updated = resource_updated
            dbdataset.fresh = fresh
        if fresh:
            revision_count += len(dataset_resources)
        else:
            last_modified_check += dataset_resources
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
    datasets = dict()
    for resource_id, url, status, result in results:
        dbresource = session.query(DBResource).filter_by(id=resource_id).first()
        dataset_id = dbresource.dataset_id
        datasetinfo = datasets.get(dataset_id, dict())
        if status == 0:
            failed_count += 1
            dbresource.error = result
            datasetinfo[resource_id] = None
        elif status == 1:
            lastmodified_count += 1
            dbresource.http_last_modified = parser.parse(result, ignoretz=True)
            set_last_modified(dbresource, dbresource.http_last_modified, 'http header')
            datasetinfo[resource_id] = dbresource.last_modified
        elif status == 2:
            if dbresource.md5_hash == result:  # File unchanged
                hash_unchanged_count += 1
            else:  # File updated
                hash_updated_count += 1
                dbresource.md5_hash = result
                dbresource.last_hash_date = datetime.datetime.utcnow()
                set_last_modified(dbresource, dbresource.last_hash_date, 'hash')
            datasetinfo[resource_id] = dbresource.last_modified
        else:
            raise ValueError('Invalid status returned!')
        datasets[dataset_id] = datasetinfo
    session.commit()

    for dataset_id in datasets:
        dbdataset = session.query(DBDataset).filter_by(id=dataset_id).first()
        dataset = datasets[dataset_id]
        dataset_last_modified = dbdataset.last_modified
        resource_updated = ''
        all_errors = True
        for resource_id in dataset:
            resource_last_modified = dataset[resource_id]
            if resource_last_modified:
                all_errors = False
                if resource_last_modified > dataset_last_modified:
                    dataset_last_modified = resource_last_modified
                    resource_updated = resource_id
        if dataset_last_modified > dbdataset.last_modified:
            dbdataset.last_modified = dataset_last_modified
            dbdataset.resource_updated = resource_updated
            if dbdataset.update_frequency is not None:
                fresh_days = datetime.timedelta(days=dbdataset.update_frequency)
                fresh_end = dataset_last_modified + fresh_days
                if fresh_end >= datetime.datetime.utcnow():
                    dbdataset.fresh = True
        dbdataset.error = all_errors
    session.commit()

    str = 'Resources\n\ndata.humdata.org: %d, manage.hdx.rwlabs.org: %d, ' % (datahumdataorg_count, managehdxrwlabsorg_count)
    str += 'proxy.hxlstandard.org: %d, scraperwiki.com: %d, ' % (proxyhxlstandardorg_count, scraperwikicom_count)
    str += 'ourairports.com: %d\n' % ourairportscom_count
    str += 'Still Fresh: %d, Revision Last Updated: %d, Last-Modified: %d, ' % (still_fresh_count, revision_count, lastmodified_count)
    str += 'Hash updated: %d, Hash Unchanged: %d\n' % (hash_updated_count, hash_unchanged_count)
    str += 'Number Failed: %d, Total resources: %d\nTotal datasets: %s' % (failed_count, total, total_datasets)
    logger.info(str)

if __name__ == '__main__':
    facade(main, hdx_site='prod')
