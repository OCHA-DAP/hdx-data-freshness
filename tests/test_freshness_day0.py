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


class TestFreshnessDay0:
    @pytest.fixture(scope='function')
    def configuration(self):
        project_config_yaml = join('..', 'config', 'project_configuration.yml')
        Configuration.create(hdx_site='prod', hdx_read_only=True, project_config_yaml=project_config_yaml)

    @pytest.fixture(scope='function')
    def nodatabase(self):
        dbpath = 'test_freshness.db'
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        return 'sqlite:///%s' % dbpath

    @pytest.fixture(scope='function')
    def now(self):
        with open('fixtures/day0/now.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def datasets(self):
        with open('fixtures/day0/datasets.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def results(self):
        with open('fixtures/day0/results.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def hash_results(self):
        with open('fixtures/day0/hash_results.pickle', 'rb') as fp:
            return pickle.load(fp)

    def test_generate_dataset(self, configuration, nodatabase, now, datasets, results, hash_results):
        freshness = Freshness(dbconn=nodatabase, datasets=datasets, now=now)
        datasets_to_check, resources_to_check = freshness.process_datasets()
        results, hash_results = freshness.check_urls(resources_to_check, results=results, hash_results=hash_results)
        datasets_lastmodified = freshness.process_results(results, hash_results)
        freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
        output = freshness.output_counts()
        assert output == '''
*** Resources ***
* total: 10193 *,
adhoc-revision: 2995,
adhoc-revision,error: 2,
adhoc-revision,hash: 71,
internal-revision: 4792,
internal-revision,api: 20,
internal-revision,hash: 130,
internal-revision,http header,hash: 6,
revision: 1676,
revision,api: 56,
revision,error: 73,
revision,hash: 290,
revision,http header: 60,
revision,http header,error: 16,
revision,http header,hash: 6

*** Datasets ***
* total: 4405 *,
0: Fresh, Updated metadata: 1915,
0: Fresh, Updated metadata,error: 8,
0: Fresh, Updated metadata,revision,http header: 9,
0: Fresh, Updated metadata,revision,http header,error: 7,
0: Fresh, Updated metadata,revision,http header,hash: 3,
1: Due, Updated metadata: 11,
2: Overdue, Updated metadata: 1662,
2: Overdue, Updated metadata,error: 10,
3: Delinquent, Updated metadata: 433,
3: Delinquent, Updated metadata,error: 4,
3: Delinquent, Updated metadata,internal-revision,http header,hash: 1,
3: Delinquent, Updated metadata,revision,http header: 3,
Freshness Unavailable, Updated metadata: 338,
Freshness Unavailable, Updated metadata,error: 1

1510 datasets have update frequency of Never'''

        dbsession = freshness.session
        dbrun = dbsession.query(DBRun).one()
        assert str(dbrun) == '<Run number=0, Run date=2017-02-01 09:07:30.333492>'
        dbresource = dbsession.query(DBResource).first()
        assert str(dbresource) == '''<Resource(run number=0, id=a67b85ee-50b4-4345-9102-d88bf9091e95, name=South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv, dataset id=84f5cc34-8a17-4e62-a868-821ff3725c0d,
url=http://data.humdata.org/dataset/84f5cc34-8a17-4e62-a868-821ff3725c0d/resource/a67b85ee-50b4-4345-9102-d88bf9091e95/download/South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv,
error=None, last modified=2017-01-25 14:38:45.135854, what updated=internal-revision,hash,
revision last updated=2017-01-25 14:38:45.135854, http last modified=2016-11-16 09:45:18, MD5 hash=2016-11-16 09:45:18, when hashed=2017-02-01 09:07:30.333492, api=False)>'''
        count = dbsession.query(DBResource).filter(DBResource.url.like('%data.humdata.org%')).count()
        assert count == 2499
        count = dbsession.query(DBResource).filter_by(what_updated='internal-revision', error=None, api=None).count()
        assert count == 4792
        count = dbsession.query(DBResource).filter_by(what_updated='internal-revision,hash', error=None, api=False).count()
        assert count == 130
        count = dbsession.query(DBResource).filter_by(what_updated='internal-revision,http header,hash', error=None, api=False).count()
        assert count == 6
        count = dbsession.query(DBResource).filter_by(what_updated='revision', error=None, api=None).count()
        assert count == 1676
        count = dbsession.query(DBResource).filter_by(what_updated='revision', error=None, api=True).count()
        assert count == 56
        count = dbsession.query(DBResource).filter(DBResource.error.isnot(None)).filter_by(what_updated='revision').count()
        assert count == 73
        count = dbsession.query(DBResource).filter_by(what_updated='revision,http header', error=None, api=None).count()
        assert count == 60
        count = dbsession.query(DBResource).filter_by(what_updated='revision,http header,hash', error=None, api=False).count()
        assert count == 6
        dbdataset = dbsession.query(DBDataset).first()
        assert str(dbdataset) == '''<Dataset(run number=0, id=84f5cc34-8a17-4e62-a868-821ff3725c0d, dataset date=07/19/2016, update frequency=0,
last_modified=2017-01-25 14:38:45.137336what updated=metadata, metadata_modified=2017-01-25 14:38:45.137336,
Resource a67b85ee-50b4-4345-9102-d88bf9091e95: last modified=2017-01-25 14:38:45.135854,
Dataset fresh=0'''
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata', error=False).count()
        assert count == 1915
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata', error=True).count()
        assert count == 8
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata,revision,http header', error=False).count()
        assert count == 9
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata,revision,http header', error=True).count()
        assert count == 7
        count = dbsession.query(DBDataset).filter_by(fresh=1, what_updated='metadata').count()
        assert count == 11
        count = dbsession.query(DBDataset).filter_by(fresh=2, what_updated='metadata', error=False).count()
        assert count == 1662
        count = dbsession.query(DBDataset).filter_by(fresh=2, what_updated='metadata', error=True).count()
        assert count == 10
        count = dbsession.query(DBDataset).filter_by(fresh=3, what_updated='metadata,internal-revision,http header,hash').count()
        assert count == 1
        count = dbsession.query(DBDataset).filter_by(fresh=None, what_updated='metadata', error=False).count()
        assert count == 338
        count = dbsession.query(DBDataset).filter_by(fresh=None, what_updated='metadata', error=True).count()
        assert count == 1
        dbinfodataset = dbsession.query(DBInfoDataset).first()
        assert str(dbinfodataset) == '''<InfoDataset(id=84f5cc34-8a17-4e62-a868-821ff3725c0d, name=south-sudan-crisis-map-explorer-data, title=South Sudan Crisis Map Explorer Data,
private=False, organization id=hdx,
maintainer=None, maintainer email=None, author=None, author email=None)>'''
        count = dbsession.query(DBInfoDataset).count()
        assert count == 4405
        dborganization = dbsession.query(DBOrganization).first()
        assert str(dborganization) == '''<Organization(id=hdx, name=hdx, title=HDX)>'''
        count = dbsession.query(DBOrganization).count()
        assert count == 179

