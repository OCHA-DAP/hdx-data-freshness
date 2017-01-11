#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for the freshness class.

'''
import os

import pickle
import shutil

import pytest
from hdx.configuration import Configuration
from os.path import join

from database.dbdataset import DBDataset
from database.dbinfodataset import DBInfoDataset
from database.dborganization import DBOrganization
from database.dbresource import DBResource
from database.dbrun import DBRun
from freshness import Freshness


class TestFreshnessDay1:
    @pytest.fixture(scope='function')
    def configuration(self):
        hdx_key_file = join('fixtures', '.hdxkey')
        project_config_yaml = join('..', 'config', 'project_configuration.yml')
        Configuration.create(hdx_site='prod', hdx_key_file=hdx_key_file, project_config_yaml=project_config_yaml)

    @pytest.fixture(scope='function')
    def database(self):
        dbpath = 'test_freshness.db'
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('fixtures', 'day0', dbpath), dbpath)
        return 'sqlite:///%s' % dbpath

    @pytest.fixture(scope='function')
    def now(self):
        with open('fixtures/day1/now.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def datasets(self):
        with open('fixtures/day1/datasets.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def results(self):
        with open('fixtures/day1/results.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def hash_results(self):
        with open('fixtures/day1/hash_results.pickle', 'rb') as fp:
            return pickle.load(fp)

    def test_generate_dataset(self, configuration, database, now, datasets, results, hash_results):
        freshness = Freshness(dbpath=database, datasets=datasets, now=now)
        datasets_to_check, resources_to_check = freshness.process_datasets()
        results, hash_results = freshness.check_urls(resources_to_check, results=results, hash_results=hash_results)
        datasets_lastmodified = freshness.process_results(results, hash_results)
        freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
        output = freshness.output_counts()
        assert output == '''
*** Resources ***
* total: 10207 *,
adhoc-nothing: 3068,
api: 7,
error: 84,
hash: 1,
internal-nothing: 4920,
internal-revision: 1,
nothing: 2115,
revision: 6,
same hash: 5

*** Datasets ***
* total: 4441 *,
0: Fresh, Updated api: 7,
0: Fresh, Updated hash: 1,
0: Fresh, Updated metadata: 3,
0: Fresh, Updated nothing: 1995,
1: Due, Updated nothing: 1711,
2: Overdue, Updated nothing: 12,
3: Delinquent, Updated nothing: 364,
Freshness Unavailable, Updated nothing: 348

1521 datasets have update frequency of Never'''

        dbsession = freshness.session
        dbrun = dbsession.query(DBRun).filter_by(run_number=1).one()
        assert str(dbrun) == '<Run number=1, Run date=2017-01-10 13:49:16.156414>'
        dbresource = dbsession.query(DBResource).first()
        assert str(dbresource) == '''<Resource(run number=0, id=33bf8136-e0ca-4d80-972e-c99f39fdc99d, name=UNOSAT_CE20130604SYR_Syria_Damage_Assessment_2016_gdb.zip, dataset id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f,
url=http://cern.ch/unosat-maps/SY/CE20130604SYR/UNOSAT_CE20130604SYR_Syria_Damage_Assessment_2016_gdb.zip,
error=None, last_modified=2017-01-09 10:22:11.181572, revision_last_updated=2017-01-09 10:22:11.181572, http_last_modified=None, MD5_hash=None, what_updated=revision)>'''
        count = dbsession.query(DBResource).filter(DBResource.url.like('%data.humdata.org%')).count()
        assert count == 4944
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='revision', error=None).count()
        assert count == 6
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='api').count()
        assert count == 7
        count = dbsession.query(DBResource).filter_by(run_number=1).filter(DBResource.error.isnot(None)).count()
        assert count == 84
        dbdataset = dbsession.query(DBDataset).first()
        assert str(dbdataset) == '''<Dataset(run number=0, id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f, dataset date=01/06/2017, update frequency=0,
last_modified=2017-01-09 11:19:19.502612what updated=metadata, metadata_modified=2017-01-09 11:19:19.502612,
Resource 33bf8136-e0ca-4d80-972e-c99f39fdc99d: last modified=2017-01-09 10:22:11.181572,
Dataset fresh=0'''
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='metadata').count()
        assert count == 3
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='nothing').count()
        assert count == 1995
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=1, what_updated='nothing').count()
        assert count == 1711
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=2, what_updated='nothing').count()
        assert count == 12
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=3, what_updated='nothing').count()
        assert count == 364
        dbinfodataset = dbsession.query(DBInfoDataset).first()
        assert str(dbinfodataset) == '''<InfoDataset(id=7d1b4f22-e0fd-400a-9fec-9db8c352c24f, name=damage-density-2016-of-idlib-idlib-governorate-syria, title=Syria - Damage density 2016 of Idlib, Idlib Governorate,
private=False, organization id=ba5aacba-0633-4364-9528-bc76a3f6cf95,
maintainer=UNOSAT, maintainer email=emergencymapping@unosat.org, author=UNITAR-UNOSAT, author email=emergencymapping@unosat.org)>'''
        count = dbsession.query(DBInfoDataset).count()
        assert count == 4441
        dborganization = dbsession.query(DBOrganization).first()
        assert str(dborganization) == '''<Organization(id=ba5aacba-0633-4364-9528-bc76a3f6cf95, name=un-operational-satellite-appplications-programme-unosat, title=UN Operational Satellite Applications Programme (UNOSAT))>'''
        count = dbsession.query(DBOrganization).count()
        assert count == 178

