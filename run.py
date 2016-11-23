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

import grequests
import requests
from hdx.utilities.downloader import Download, DownloadError
from requests.adapters import HTTPAdapter
from requests.packages.urllib3 import Retry
from sqlalchemy.orm import sessionmaker
from dateutil import parser
from sqlalchemy import create_engine

from hdx.data.resource import Resource
from hdx.facades.simple import facade
from database.base import Base
from database.dbresource import DBResource

logger = logging.getLogger(__name__)
engine = create_engine('sqlite:///resources.db', echo=False)
Session = sessionmaker(bind=engine)


def set_metadata(metadata):
    def hook(resp, **kwargs):
        resp.metadata = metadata
        return resp
    return hook


def check_resources_for_last_modified(last_modified_check):
    results = list()
    reqs = set()

    def exception_handler(req, _):
        resource_id, url = req.metadata
        results.append((resource_id, url, 0, None))

    with Download() as download:
        for metadata in last_modified_check:
            req = grequests.get(metadata[1], session=download.session, callback=set_metadata(metadata))
            req.metadata = metadata
            reqs.add(req)
        for resp in grequests.imap(reqs, size=5000, stream=True, exception_handler=exception_handler):
            resource_id, url = resp.metadata
            last_modified = resp.headers.get('Last-Modified', None)
            if last_modified:
                results.append((resource_id, url, 1, last_modified))
                resp.close()
                continue
            download.response = resp
            try:
                md5hash = download.hash_stream(url)
                results.append((resource_id, url, 2, md5hash))
            except DownloadError:
                results.append((resource_id, url, 0, None))
            finally:
                resp.close()
    return results


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
        last_modified_check.append((resource_id, url))
    session.commit()
    start_time = time.time()
    results = check_resources_for_last_modified(last_modified_check)
    logger.info('Execution time: %s seconds' % (time.time() - start_time))
    lastmodified_count = 0
    hash_updated_count = 0
    hash_unchanged_count = 0
    failed_count = 0
    count = 0
    for resource_id, url, status, result in results:
        logger.info('Count = %d' % count)
        count += 1
        dbresource = session.query(DBResource).filter_by(id=resource_id).first()
        if status == 0:
            failed_count += 1
            dbresource.broken_url = True
        elif status == 1:
            lastmodified_count += 1
            set_last_modified(dbresource, result)
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
