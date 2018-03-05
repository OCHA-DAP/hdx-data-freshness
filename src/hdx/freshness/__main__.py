#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import argparse
import logging
import os
import time
from urllib.parse import urlparse

import psycopg2
from hdx.hdx_configuration import Configuration
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from hdx.freshness.datafreshness import DataFreshness
from hdx.freshness.testdata.testbase import TestBase

setup_logging()
logger = logging.getLogger(__name__)


def main(hdx_key, user_agent, preprefix, hdx_site, db_url, save):
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    site_url = Configuration.create(hdx_key=hdx_key, hdx_site=hdx_site,
                                    user_agent=user_agent, preprefix=preprefix,
                                    project_config_yaml=project_config_yaml)
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % site_url)
    if db_url:
        logger.info('> DB URL: %s' % db_url)
        if 'postgres' in db_url:
            result = urlparse(db_url)
            username = result.username
            password = result.password
            database = result.path[1:]
            hostname = result.hostname
            connecting_string = 'Checking for PostgreSQL...'
            while True:
                try:
                    logger.info(connecting_string)
                    connection = psycopg2.connect(
                        database=database,
                        user=username,
                        password=password,
                        host=hostname,
                        connect_timeout=3
                    )
                    connection.close()
                    logger.info('PostgreSQL is running!')
                    break
                except psycopg2.OperationalError:
                    time.sleep(1)
    testsession = None
    if save:
        engine = create_engine('sqlite:///test_serialize.db', poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        TestBase.metadata.create_all(engine)
        testsession = Session()
    freshness = DataFreshness(db_url=db_url, testsession=testsession)

    datasets_to_check, resources_to_check = freshness.process_datasets()
    results, hash_results = freshness.check_urls(resources_to_check)
    datasets_lastmodified = freshness.process_results(results, hash_results)
    freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
    freshness.output_counts()
    freshness.close()
    logger.info('Freshness completed!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-s', '--save', default=False, action='store_true', help='Save state for testing')
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = os.getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = os.getenv('USER_AGENT')
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = os.getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = os.getenv('HDX_SITE', 'prod')
    db_url = args.db_url
    if db_url is None:
        db_url = os.getenv('DB_URL')
    if db_url and '://' not in db_url:
        db_url = 'postgresql://%s' % db_url
    main(hdx_key, user_agent, preprefix, hdx_site, db_url, args.save)
