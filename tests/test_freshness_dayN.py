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


class TestFreshnessDayN:
    @pytest.fixture(scope='function')
    def configuration(self):
        project_config_yaml = join('..', 'config', 'project_configuration.yml')
        Configuration.create(hdx_site='prod', hdx_read_only=True, project_config_yaml=project_config_yaml)

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
        with open('fixtures/dayN/now.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def datasets(self):
        with open('fixtures/dayN/datasets.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def results(self):
        with open('fixtures/dayN/results.pickle', 'rb') as fp:
            return pickle.load(fp)

    @pytest.fixture(scope='function')
    def hash_results(self):
        with open('fixtures/dayN/hash_results.pickle', 'rb') as fp:
            return pickle.load(fp)

    def test_generate_dataset(self, configuration, database, now, datasets, results, hash_results):
        freshness = Freshness(db_url=database, datasets=datasets, now=now)
        datasets_to_check, resources_to_check = freshness.process_datasets()
        results, hash_results = freshness.check_urls(resources_to_check, results=results, hash_results=hash_results)
        datasets_lastmodified = freshness.process_results(results, hash_results)
        freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
        output = freshness.output_counts()
        assert output == '''
*** Resources ***
* total: 10192 *,
adhoc-nothing: 2986,
adhoc-nothing,error: 2,
adhoc-nothing,hash: 80,
api: 53,
error: 123,
hash: 113,
http header,hash: 1,
internal-nothing: 4887,
internal-nothing,hash: 57,
internal-nothing,http header,hash: 2,
internal-revision: 1,
nothing: 1703,
revision: 4,
same hash: 180

*** Datasets ***
* total: 4404 *,
0: Fresh, Updated hash: 2,
0: Fresh, Updated internal-nothing,http header,hash: 1,
0: Fresh, Updated metadata: 3,
0: Fresh, Updated nothing: 1897,
0: Fresh, Updated nothing,error: 41,
1: Due, Updated nothing: 10,
2: Overdue, Updated nothing: 1660,
2: Overdue, Updated nothing,error: 10,
3: Delinquent, Updated nothing: 437,
3: Delinquent, Updated nothing,error: 4,
Freshness Unavailable, Updated nothing: 338,
Freshness Unavailable, Updated nothing,error: 1

1510 datasets have update frequency of Never'''

        dbsession = freshness.session
        dbrun = dbsession.query(DBRun).filter_by(run_number=1).one()
        assert str(dbrun) == '<Run number=1, Run date=2017-02-02 07:46:52.552929>'
        dbresource = dbsession.query(DBResource).first()
        assert str(dbresource) == '''<Resource(run number=0, id=a67b85ee-50b4-4345-9102-d88bf9091e95, name=South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv, dataset id=84f5cc34-8a17-4e62-a868-821ff3725c0d,
url=http://data.humdata.org/dataset/84f5cc34-8a17-4e62-a868-821ff3725c0d/resource/a67b85ee-50b4-4345-9102-d88bf9091e95/download/South_Sudan_Recent_Conflict_Event_Total_Fatalities.csv,
error=None, last modified=2017-01-25 14:38:45.135854, what updated=internal-revision,hash,
revision last updated=2017-01-25 14:38:45.135854, http last modified=2016-11-16 09:45:18, MD5 hash=2016-11-16 09:45:18, when hashed=2017-02-01 09:07:30.333492, api=False)>'''
        count = dbsession.query(DBResource).filter(DBResource.url.like('%data.humdata.org%')).count()
        assert count == 4997
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='revision', error=None).count()
        assert count == 4
        count = dbsession.query(DBResource).filter_by(run_number=1, api=True).count()
        assert count == 53
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='adhoc-nothing').filter(DBResource.error.isnot(None)).count()
        assert count == 2
        hash_updated = dbsession.query(DBResource.id).filter_by(run_number=1).filter(DBResource.what_updated.like('%hash%'))
        assert hash_updated.count() == 253
        count = dbsession.query(DBResource).filter_by(run_number=0).filter(DBResource.md5_hash.isnot(None)).filter(DBResource.id.in_(hash_updated.as_scalar())).count()
        assert count == 2
        # select what_updated, api from dbresources where run_number=0 and md5_hash is not null and id in (select id from dbresources where run_number=1 and what_updated like '%hash%');
        dbdataset = dbsession.query(DBDataset).first()
        assert str(dbdataset) == '''<Dataset(run number=0, id=84f5cc34-8a17-4e62-a868-821ff3725c0d, dataset date=07/19/2016, update frequency=0,
last_modified=2017-01-25 14:38:45.137336what updated=metadata, metadata_modified=2017-01-25 14:38:45.137336,
Resource a67b85ee-50b4-4345-9102-d88bf9091e95: last modified=2017-01-25 14:38:45.135854,
Dataset fresh=0'''
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='metadata').count()
        assert count == 3
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='nothing', error=False).count()
        assert count == 1897
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=1, what_updated='nothing').count()
        assert count == 10
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=2, what_updated='nothing', error=True).count()
        assert count == 10
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=3, what_updated='nothing', error=False).count()
        assert count == 437
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=None, what_updated='nothing', error=False).count()
        assert count == 338
        dbinfodataset = dbsession.query(DBInfoDataset).first()
        assert str(dbinfodataset) == '''<InfoDataset(id=84f5cc34-8a17-4e62-a868-821ff3725c0d, name=south-sudan-crisis-map-explorer-data, title=South Sudan Crisis Map Explorer Data,
private=False, organization id=hdx,
maintainer=mcarans, maintainer email=None, author=None, author email=None)>'''
        count = dbsession.query(DBInfoDataset).count()
        assert count == 4405
        dborganization = dbsession.query(DBOrganization).first()
        assert str(dborganization) == '''<Organization(id=hdx, name=hdx, title=HDX)>'''
        count = dbsession.query(DBOrganization).count()
        assert count == 179
