#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the freshness class.

'''
import os

import pickle
import pytest
from hdx.configuration import Configuration
from os.path import join

from database.dbdataset import DBDataset
from database.dbinfodataset import DBInfoDataset
from database.dborganization import DBOrganization
from database.dbresource import DBResource
from database.dbrun import DBRun
from freshness import Freshness


class TestFreshnessDay0():
    @pytest.fixture(scope='function')
    def configuration(self):
        project_config_yaml = join('..', 'config', 'project_configuration.yml')
        Configuration.create(hdx_site='prod', project_config_yaml=project_config_yaml)

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
        freshness = Freshness(dbpath=nodatabase, datasets=datasets, now=now)
        datasets_to_check, resources_to_check = freshness.process_datasets()
        results, hash_results = freshness.check_urls(resources_to_check, results=results, hash_results=hash_results)
        datasets_lastmodified = freshness.process_results(results, hash_results)
        freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
        output = freshness.output_counts()
        assert output == '''
*** Resources ***
* total: 10205 *,
adhoc-revision: 3068,
internal-revision: 4921,
revision: 1829,
revision,api: 47,
revision,error: 86,
revision,hash: 192,
revision,http header: 62

*** Datasets ***
* total: 4440 *,
0: Fresh, Updated metadata: 1883,
0: Fresh, Updated metadata,revision,api: 15,
0: Fresh, Updated metadata,revision,hash: 100,
0: Fresh, Updated metadata,revision,http header: 8,
1: Due, Updated metadata: 1710,
2: Overdue, Updated metadata: 12,
3: Delinquent, Updated metadata: 361,
3: Delinquent, Updated metadata,revision,http header: 3,
Freshness Unavailable, Updated metadata: 348

1521 datasets have update frequency of Never'''

        dbsession = freshness.session
        dbrun = dbsession.query(DBRun).one()
        assert str(dbrun) == '<Run number=0, Run date=2017-01-09 12:01:13.932811>'
        dbresource = dbsession.query(DBResource).first()
        assert str(dbresource) == '''<Resource(run number=0, id=33bf8136-e0ca-4d80-972e-c99f39fdc99d, name=UNOSAT_CE20130604SYR_Syria_Damage_Assessment_2016_gdb.zip, dataset id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f,
url=http://cern.ch/unosat-maps/SY/CE20130604SYR/UNOSAT_CE20130604SYR_Syria_Damage_Assessment_2016_gdb.zip,
error=None, last_modified=2017-01-09 10:22:11.181572, revision_last_updated=2017-01-09 10:22:11.181572, http_last_modified=None, MD5_hash=None, what_updated=revision)>'''
        count = dbsession.query(DBResource).filter(DBResource.url.like('%data.humdata.org%')).count()
        assert count == 2472
        count = dbsession.query(DBResource).filter_by(what_updated='revision', error=None).count()
        assert count == 1829
        count = dbsession.query(DBResource).filter_by(what_updated='revision,api').count()
        assert count == 47
        count = dbsession.query(DBResource).filter(DBResource.error.isnot(None)).filter_by(what_updated='revision').count()
        assert count == 86
        dbdataset = dbsession.query(DBDataset).first()
        assert str(dbdataset) == '''<Dataset(run number=0, id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f, dataset date=01/06/2017, update frequency=0,
last_modified=2017-01-09 11:19:19.502612what updated=metadata, metadata_modified=2017-01-09 11:19:19.502612,
Resource 33bf8136-e0ca-4d80-972e-c99f39fdc99d: last modified=2017-01-09 10:22:11.181572,
Dataset fresh=0'''
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata').count()
        assert count == 1883
        count = dbsession.query(DBDataset).filter_by(fresh=0, what_updated='metadata,revision,hash').count()
        assert count == 100
        count = dbsession.query(DBDataset).filter_by(fresh=1, what_updated='metadata').count()
        assert count == 1710
        count = dbsession.query(DBDataset).filter_by(fresh=2, what_updated='metadata').count()
        assert count == 12
        count = dbsession.query(DBDataset).filter_by(fresh=3, what_updated='metadata,revision,http header').count()
        assert count == 3
        dbinfodataset = dbsession.query(DBInfoDataset).first()
        assert str(dbinfodataset) == '''<InfoDataset(id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f, name=damage-density-2016-of-idlib-idlib-governorate-syria, title=Syria - Damage density 2016 of Idlib, Idlib Governorate,
private=False, organization id=ba5aacba-0633-4364-9528-bc76a3f6cf95,
maintainer=None, maintainer email=None, author=None, author email=None)>'''
        count = dbsession.query(DBInfoDataset).count()
        assert count == 4440
        dborganization = dbsession.query(DBOrganization).first()
        assert str(dborganization) == '''<Organization(id=ba5aacba-0633-4364-9528-bc76a3f6cf95, name=un-operational-satellite-appplications-programme-unosat, title=UN Operational Satellite Applications Programme (UNOSAT))>'''
        count = dbsession.query(DBOrganization).count()
        assert count == 178

