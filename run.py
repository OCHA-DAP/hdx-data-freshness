#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import logging
import time
import datetime

from deco import concurrent, synchronized
from sqlalchemy.orm import sessionmaker
from dateutil import parser
from sqlalchemy import create_engine

from hdx.data.resource import Resource
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download, DownloadError
from database.base import Base
from database.dbresource import DBResource

logger = logging.getLogger(__name__)
engine = create_engine('sqlite:///resources.db', echo=False)
Session = sessionmaker(bind=engine)


@concurrent
def get_headers_or_hash(url):
    try:
        with Download() as download:
            download.setup_stream(url, 10)
            last_modified = download.response.headers.get('Last-Modified', None)
            if last_modified:
                return url, 1, last_modified
            md5hash = download.hash_stream(url)
            return url, 2, md5hash
    except DownloadError:
        return url, 0, None


@synchronized
def check_resources_for_last_modified(last_modified_check):
    results = dict()
    for resource_id in last_modified_check:
        url = last_modified_check[resource_id]
        results[resource_id] = get_headers_or_hash(url)
    return results


def set_last_modified(dbresource, resource_id, modified_date):
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
    last_modified_check = dict()
    for resource in resources:
        url = resource['url']
        resource_id = resource['id']
        revision_last_updated = resource.get('revision_last_updated', None)
        if revision_last_updated:
            revision_last_updated = parser.parse(revision_last_updated, ignoretz=True)
        dbresource = DBResource(id=resource_id, name=resource['name'], url=url,
                                last_modified=revision_last_updated, revision_last_updated=revision_last_updated)
        session.add(dbresource)
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
        last_modified_check[resource_id] = url
    session.commit()
    start_time = time.time()
    results = check_resources_for_last_modified(last_modified_check)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    lastmodified_count = 0
    hash_updated_count = 0
    hash_unchanged_count = 0
    failed_count = 0
    for resource_id in results:
        url, status, result = results[resource_id]
        dbresource = session.query(DBResource).filter_by(id=resource_id).first()
        if status == 0:
            failed_count += 1
            dbresource.broken_url = True
        elif status == 1:
            lastmodified_count += 1
            set_last_modified(dbresource, resource_id, result)
        elif status == 2:
            if dbresource.md5_hash == result:  # File unchanged
                hash_unchanged_count += 1
            else:  # File updated
                dbresource.md5_hash = result
                dbresource.last_hash_date = datetime.date.today()
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
