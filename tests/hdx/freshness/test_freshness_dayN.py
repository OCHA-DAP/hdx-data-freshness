# -*- coding: utf-8 -*-
'''
Unit tests for the freshness class.

'''
import os
import shutil
from os.path import join

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.datafreshness import DataFreshness
from hdx.freshness.testdata.dbtestresult import DBTestResult
from hdx.freshness.testdata.serialize import deserialize_now, deserialize_datasets, deserialize_results, \
    deserialize_hashresults
from hdx.freshness.testdata.testbase import TestBase


class TestFreshnessDayN:
    @pytest.fixture(scope='function')
    def database(self):
        dbfile = 'test_freshness.db'
        dbpath = join('tests', dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join('tests', 'fixtures', 'day0', dbfile), dbpath)
        return 'sqlite:///%s' % dbpath

    @pytest.fixture(scope='class')
    def serializedbsession(self):
        dbpath = join('tests', 'fixtures', 'dayN', 'test_serialize.db')
        engine = create_engine('sqlite:///%s' % dbpath, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        TestBase.metadata.create_all(engine)
        return Session()

    @pytest.fixture(scope='function')
    def now(self, serializedbsession):
        return deserialize_now(serializedbsession)

    @pytest.fixture(scope='function')
    def datasets(self, serializedbsession):
        return deserialize_datasets(serializedbsession)

    @pytest.fixture(scope='function')
    def results(self, serializedbsession):
        return deserialize_results(serializedbsession)

    @pytest.fixture(scope='function')
    def hash_results(self, serializedbsession):
        return deserialize_hashresults(serializedbsession)

    @pytest.fixture(scope='function')
    def forced_hash_ids(self, serializedbsession):
        forced_hash_ids = serializedbsession.query(DBTestResult.id).filter_by(force_hash=1)
        return [x[0] for x in forced_hash_ids]

    def test_generate_dataset(self, configuration, database, now, datasets, results, hash_results, forced_hash_ids,
                              resourcecls):
        freshness = DataFreshness(db_url=database, datasets=datasets, now=now)
        freshness.spread_datasets()
        freshness.add_new_run()
        dbsession = freshness.session
        dbsession.execute(
            "INSERT INTO dbresources(run_number,id,name,dataset_id,url,error,last_modified,what_updated,revision_last_updated,http_last_modified,md5_hash,when_hashed,when_checked,api) VALUES (-1,'010ab2d2-8f98-409b-a1f0-4707ad6c040a','sidih_190.csv','54d6b4b8-8cc9-42d3-82ce-3fa4fd3d9be1','https://ds-ec2.scraperwiki.com/egzfk1p/siqsxsgjnxgk3r2/cgi-bin/csv/sidih_190.csv',NULL,'2015-05-07 14:44:56.599079','','2015-05-07 14:44:56.599079',NULL,'999','2017-12-16 16:03:33.208327','2017-12-16 16:03:33.208327','0');")
        datasets_to_check, resources_to_check = freshness.process_datasets(forced_hash_ids=forced_hash_ids)
        results, hash_results = freshness.check_urls(resources_to_check, results=results, hash_results=hash_results)
        datasets_lastmodified = freshness.process_results(results, hash_results, resourcecls=resourcecls)
        freshness.update_dataset_last_modified(datasets_to_check, datasets_lastmodified)
        output = freshness.output_counts()
        assert output == '''
*** Resources ***
* total: 660 *,
api: 4,
error: 14,
hash: 3,
http header: 1,
internal-nothing: 45,
internal-nothing,error: 2,
internal-revision: 9,
nothing: 575,
repeat hash: 1,
same hash: 6

*** Datasets ***
* total: 103 *,
0: Fresh, Updated hash: 1,
0: Fresh, Updated http header: 1,
0: Fresh, Updated metadata: 3,
0: Fresh, Updated nothing: 70,
2: Overdue, Updated nothing: 1,
3: Delinquent, Updated nothing: 19,
3: Delinquent, Updated nothing,error: 4,
Freshness Unavailable, Updated nothing: 3,
Freshness Unavailable, Updated nothing,error: 1

15 datasets have update frequency of Live
19 datasets have update frequency of Never
0 datasets have update frequency of Adhoc'''

        dbrun = dbsession.query(DBRun).filter_by(run_number=1).one()
        assert str(dbrun) == '<Run number=1, Run date=2017-12-19 10:53:28.606889>'
        dbresource = dbsession.query(DBResource).first()
        assert str(dbresource) == '''<Resource(run number=0, id=b21d6004-06b5-41e5-8e3e-0f28140bff64, name=Topline Numbers.csv, dataset id=a2150ad9-2b87-49f5-a6b2-c85dff366b75,
url=https://docs.google.com/spreadsheets/d/e/2PACX-1vRjFRZGLB8IMp0anSGR1tcGxwJgkyx0bTN9PsinqtaLWKHBEfz77LkinXeVqIE_TsGVt-xM6DQzXpkJ/pub?gid=0&single=true&output=csv,
error=None, last modified=2017-12-16 15:11:15.202742, what updated=revision,hash,
revision last updated=2017-12-16 15:11:15.202742, http last modified=None, MD5 hash=None, when hashed=2017-12-18 16:03:33.208327, when checked=2017-12-18 16:03:33.208327, api=False)>'''
        count = dbsession.query(DBResource).filter(DBResource.url.like('%data.humdata.org%')).count()
        assert count == 112
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='revision', error=None).count()
        assert count == 0
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='hash', error=None).count()
        assert count == 3
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='http header', error=None).count()
        assert count == 1
        count = dbsession.query(DBResource).filter_by(run_number=1, api=True).count()
        assert count == 4
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='adhoc-nothing').filter(DBResource.error.isnot(None)).count()
        assert count == 0
        count = dbsession.query(DBResource).filter_by(run_number=1, what_updated='internal-nothing').filter(
            DBResource.error.isnot(None)).count()
        assert count == 2
        # select what_updated, api from dbresources where run_number=0 and md5_hash is not null and id in (select id from dbresources where run_number=1 and what_updated like '%hash%');
        hash_updated = dbsession.query(DBResource.id).filter_by(run_number=1).filter(DBResource.what_updated.like('%hash%'))
        assert hash_updated.count() == 4
        count = dbsession.query(DBResource).filter_by(run_number=0).filter(DBResource.md5_hash.isnot(None)).filter(DBResource.id.in_(hash_updated.as_scalar())).count()
        assert count == 2
        dbdataset = dbsession.query(DBDataset).first()
        assert str(dbdataset) == '''<Dataset(run number=0, id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, dataset date=09/21/2017, update frequency=1,
last_modified=2017-12-16 15:11:15.204215what updated=metadata, metadata_modified=2017-12-16 15:11:15.204215,
Resource b21d6004-06b5-41e5-8e3e-0f28140bff64: last modified=2017-12-16 15:11:15.202742,
Dataset fresh=2'''
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='metadata').count()
        assert count == 3
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=0, what_updated='nothing', error=False).count()
        assert count == 70
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=1, what_updated='nothing').count()
        assert count == 0
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=2, what_updated='nothing', error=False).count()
        assert count == 1
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=3, what_updated='nothing', error=False).count()
        assert count == 19
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=3, what_updated='nothing', error=True).count()
        assert count == 4
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=None, what_updated='nothing', error=False).count()
        assert count == 3
        count = dbsession.query(DBDataset).filter_by(run_number=1, fresh=None, what_updated='nothing',
                                                     error=True).count()
        assert count == 1
        dbinfodataset = dbsession.query(DBInfoDataset).first()
        assert str(dbinfodataset) == '''<InfoDataset(id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, name=rohingya-displacement-topline-figures, title=Rohingya Displacement Topline Figures,
private=False, organization id=hdx,
maintainer=7d7f5f8d-7e3b-483a-8de1-2b122010c1eb, maintainer email=takavarasha@un.org, author=None, author email=None, location=bgd)>'''
        count = dbsession.query(DBInfoDataset).count()
        assert count == 103
        dborganization = dbsession.query(DBOrganization).first()
        assert str(dborganization) == '''<Organization(id=hdx, name=hdx, title=HDX)>'''
        count = dbsession.query(DBOrganization).count()
        assert count == 40
