#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import argparse
import logging

from hdx.configuration import Configuration
from hdx.logging import setup_logging

from freshness import Freshness

setup_logging()
logger = logging.getLogger(__name__)


def main(dbconn, save):
    configuration = Configuration.create(hdx_read_only=True, hdx_site='prod')
    logger.info('--------------------------------------------------')
    logger.info('> HDX Site: %s' % configuration.get_hdx_site_url())
    freshness = Freshness(dbconn=dbconn, save=save)  # 'postgresql://postgres@hdxdatafreshness_db_1:5432'
    datasets_to_check, resources_to_check = freshness.process_datasets()
    results, hash_results = freshness.check_urls(resources_to_check)
    datasets_lastmodified = freshness.process_results(results, hash_results)
    freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
    freshness.output_counts()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Freshness')
    parser.add_argument('-c', '--dbconn', default=None, help='Database connection string')
    parser.add_argument('-s', '--save', default=False, action='store_true', help='Save state for testing')
    args = parser.parse_args()
    main(args.dbconn, args.save)
