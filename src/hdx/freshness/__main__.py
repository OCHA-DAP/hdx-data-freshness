#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import argparse
import logging
from os import getenv

from hdx.facades.keyword_arguments import facade
from hdx.utilities.database import Database
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

from hdx.freshness.datafreshness import DataFreshness
from hdx.freshness.version import get_freshness_version

setup_logging()
logger = logging.getLogger(__name__)


def main(db_url, db_params, do_touch, save, **ignore):
    logger.info('> Data freshness %s' % get_freshness_version())
    if db_params:
        params = args_to_dict(db_params)
    elif db_url:
        params = Database.get_params_from_sqlalchemy_url(db_url)
    else:
        params = {'driver': 'sqlite', 'database': 'freshness.db'}
    logger.info('> Database parameters: %s' % params)
    with Database(**params) as session:
        testsession = None
        if save:
            testsession = Database.get_session('sqlite:///test_serialize.db')
        freshness = DataFreshness(session=session, testsession=testsession, do_touch=do_touch)
        freshness.spread_datasets()
        freshness.add_new_run()
        datasets_to_check, resources_to_check = freshness.process_datasets()
        results, hash_results = freshness.check_urls(resources_to_check, UserAgent.get())
        datasets_lastmodified = freshness.process_results(results, hash_results)
        freshness.update_dataset_latest_of_modifieds(datasets_to_check, datasets_lastmodified)
        freshness.output_counts()
        if testsession:
            testsession.close()
    logger.info('Freshness completed!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-ua', '--user_agent', default=None, help='user agent')
    parser.add_argument('-pp', '--preprefix', default=None, help='preprefix')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-db', '--db_url', default=None, help='Database connection string')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters. Overrides --db_url.')
    parser.add_argument('-dt', '--donttouch', default=False, action='store_true', help="Don't touch datasets")
    parser.add_argument('-s', '--save', default=False, action='store_true', help='Save state for testing')
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv('HDX_KEY')
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv('USER_AGENT')
        if user_agent is None:
            user_agent = 'freshness'
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv('PREPREFIX')
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv('HDX_SITE', 'prod')
    db_url = args.db_url
    if db_url is None:
        db_url = getenv('DB_URL')
    if db_url and '://' not in db_url:
        db_url = 'postgresql://%s' % db_url
    project_config_yaml = script_dir_plus_file('project_configuration.yml', main)
    facade(main, hdx_key=hdx_key, user_agent=user_agent, preprefix=preprefix, hdx_site=hdx_site,
           project_config_yaml=project_config_yaml, db_url=db_url, db_params=args.db_params,
           do_touch=not args.donttouch, save=args.save)
