#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the freshness class.

'''
import os

import pickle
from datetime import timedelta

import pytest
from hdx.configuration import Configuration
from os.path import join

from database.dbdataset import DBDataset
from database.dbinfodataset import DBInfoDataset
from database.dborganization import DBOrganization
from database.dbresource import DBResource
from database.dbrun import DBRun
from freshness import Freshness


class TestAging:
    @pytest.fixture(scope='class')
    def configuration(self):
        project_config_yaml = join('..', 'config', 'project_configuration.yml')
        Configuration.create(hdx_site='prod', hdx_read_only=True, project_config_yaml=project_config_yaml)

    @pytest.fixture(scope='class')
    def now(self):
        with open('fixtures/day0/now.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='class')
    def nodatabase(self):
        dbpath = 'test_freshness.db'
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        return 'sqlite:///%s' % dbpath

    @pytest.fixture(scope='class')
    def datasets(self):
        with open('fixtures/day0/datasets.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.mark.parametrize("days_last_modified,update_frequency,expected_status", [
        (0, 1, 0),
        (0, 7, 0),
        (0, 365, 0),
        (1, 1, 1),
        (1, 7, 0),
        (2, 1, 2),
        (3, 1, 3),
        (6, 7, 0),
        (7, 7, 1),
        (8, 7, 1),
        (13, 7, 1),
        (13, 14, 0),
        (14, 7, 2),
        (14, 14, 1),
        (20, 7, 2),
        (20, 14, 1),
        (21, 7, 3),
        (21, 14, 2),
        (29, 14, 3),
        (29, 30, 0),
        (30, 30, 1),
        (30, 90, 0),
        (45, 30, 2),
        (45, 90, 0),
        (60, 30, 3),
        (60, 90, 0),
        (89, 90, 0),
        (90, 90, 1),
        (90, 90, 1),
        (120, 90, 2),
        (150, 90, 3),
        (179, 180, 0),
        (180, 180, 1),
        (210, 180, 2),
        (210, 365, 0),
        (240, 180, 3),
        (240, 365, 0),
        (364, 365, 0),
        (365, 365, 1),
        (425, 365, 2),
        (455, 365, 3),
    ])
    def test_aging(self, configuration, nodatabase, now, datasets,
                   days_last_modified, update_frequency, expected_status):
        freshness = Freshness(dbconn=nodatabase, datasets=datasets, now=now)
        last_modified = now - timedelta(days=days_last_modified)
        status = freshness.calculate_aging(last_modified, update_frequency)
        assert status == expected_status
