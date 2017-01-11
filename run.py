#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
REGISTER:
---------

Caller script. Designed to call all other functions.

'''
import logging

from hdx.facades.simple import facade

from freshness import Freshness

logger = logging.getLogger(__name__)

def main():
    freshness = Freshness(save=True)
    datasets_to_check, resources_to_check = freshness.process_datasets()
    results, hash_results = freshness.check_urls(resources_to_check)
    datasets_lastmodified = freshness.process_results(results, hash_results)
    freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
    freshness.output_counts()

if __name__ == '__main__':
    facade(main, hdx_read_only=True, hdx_site='prod')
