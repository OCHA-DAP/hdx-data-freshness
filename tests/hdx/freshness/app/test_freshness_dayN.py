"""
Unit tests for the freshness class.

Takes base database from fixtures/day0/test_freshness.db

Run data comes from fixtures/dayN/test_serialize.db

When adding new test data, remember that self.now (run date) is 2017-12-19 10:53:28.606889

Set force_hash in dbtestresults and dbtesthashresults if dataset is fresh or you'll get duplicates
(number of resources won't sum to total)!
"""
import os
import shutil
from os.path import join

import pytest
from hdx.database import Database

from hdx.freshness.app.datafreshness import DataFreshness
from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.testdata.dbtestresult import DBTestResult
from hdx.freshness.testdata.serialize import (
    deserialize_datasets,
    deserialize_hashresults,
    deserialize_now,
    deserialize_results,
)


class TestFreshnessDayN:
    @pytest.fixture(scope="function")
    def database(self):
        dbfile = "test_freshness.db"
        dbpath = join("tests", dbfile)
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        shutil.copyfile(join("tests", "fixtures", "day0", dbfile), dbpath)
        return {"driver": "sqlite", "database": dbpath}

    @pytest.fixture(scope="class")
    def serializedbsession(self):
        dbpath = join("tests", "fixtures", "dayN", "test_serialize.db")
        return Database.get_session(f"sqlite:///{dbpath}")

    @pytest.fixture(scope="function")
    def now(self, serializedbsession):
        return deserialize_now(serializedbsession)

    @pytest.fixture(scope="function")
    def datasets(self, serializedbsession):
        return deserialize_datasets(serializedbsession)

    @pytest.fixture(scope="function")
    def results(self, serializedbsession):
        return deserialize_results(serializedbsession)

    @pytest.fixture(scope="function")
    def hash_results(self, serializedbsession):
        return deserialize_hashresults(serializedbsession)

    @pytest.fixture(scope="function")
    def forced_hash_ids(self, serializedbsession):
        forced_hash_ids = serializedbsession.query(DBTestResult.id).filter_by(
            force_hash=1
        )
        return [x[0] for x in forced_hash_ids]

    def test_generate_dataset(
        self,
        configuration,
        database,
        now,
        datasets,
        results,
        hash_results,
        forced_hash_ids,
        resourcecls,
    ):
        with Database(**database) as session:
            freshness = DataFreshness(
                session=session, datasets=datasets, now=now, do_touch=True
            )
            freshness.spread_datasets()
            freshness.add_new_run()
            dbsession = freshness.session
            # insert resource with run number -1 with hash 999 to test repeated hash
            dbsession.execute(
                "INSERT INTO dbresources(run_number,id,name,dataset_id,url,last_modified,metadata_modified,latest_of_modifieds,what_updated,http_last_modified,md5_hash,hash_last_modified,when_checked,api,error) VALUES (-1,'010ab2d2-8f98-409b-a1f0-4707ad6c040a','sidih_190.csv','54d6b4b8-8cc9-42d3-82ce-3fa4fd3d9be1','https://ds-ec2.scraperwiki.com/egzfk1p/siqsxsgjnxgk3r2/cgi-bin/csv/sidih_190.csv','2015-05-07 14:44:56.599079','2015-05-07 14:44:56.599079','2015-05-07 14:44:56.599079','',NULL,'999','2017-12-16 16:03:33.208327','2017-12-16 16:03:33.208327','0',NULL);"
            )
            datasets_to_check, resources_to_check = freshness.process_datasets(
                hash_ids=forced_hash_ids
            )
            results, hash_results = freshness.check_urls(
                resources_to_check,
                "test",
                results=results,
                hash_results=hash_results,
            )
            resourcecls.populate_resourcedict(datasets)
            datasets_lastmodified = freshness.process_results(
                results, hash_results, resourcecls=resourcecls
            )
            freshness.update_dataset_latest_of_modifieds(
                datasets_to_check, datasets_lastmodified
            )
            output = freshness.output_counts()
            # Make sure the sum of the resources = the total resources and sum of the datasets = the total datasets!
            assert (
                output
                == """
*** Resources ***
* total: 657 *,
api: 3,
error: 26,
first hash: 4,
hash: 3,
internal-filestore: 10,
internal-nothing: 46,
nothing: 558,
repeat hash: 1,
same hash: 6

*** Datasets ***
* total: 104 *,
0: Fresh, Updated filestore: 4,
0: Fresh, Updated filestore,review date: 1,
0: Fresh, Updated firstrun: 1,
0: Fresh, Updated hash: 3,
0: Fresh, Updated nothing: 59,
0: Fresh, Updated review date: 1,
0: Fresh, Updated script update: 1,
1: Due, Updated nothing: 1,
2: Overdue, Updated nothing: 1,
3: Delinquent, Updated nothing: 23,
3: Delinquent, Updated nothing,error: 4,
Freshness Unavailable, Updated no resources: 1,
Freshness Unavailable, Updated nothing: 4

15 datasets have update frequency of Live
19 datasets have update frequency of Never
0 datasets have update frequency of As Needed"""
            )

            dbrun = dbsession.query(DBRun).filter_by(run_number=1).one()
            assert (
                str(dbrun)
                == "<Run number=1, Run date=2017-12-19 10:53:28.606889>"
            )
            dbresource = dbsession.query(DBResource).first()
            assert (
                str(dbresource)
                == """<Resource(run number=0, id=b21d6004-06b5-41e5-8e3e-0f28140bff64, name=Topline Numbers.csv, dataset id=a2150ad9-2b87-49f5-a6b2-c85dff366b75,
url=https://docs.google.com/spreadsheets/d/e/2PACX-1vRjFRZGLB8IMp0anSGR1tcGxwJgkyx0bTN9PsinqtaLWKHBEfz77LkinXeVqIE_TsGVt-xM6DQzXpkJ/pub?gid=0&single=true&output=csv,
last modified=2017-12-16 15:11:15.202742, metadata modified=2017-12-16 15:11:15.202742,
latest of modifieds=2017-12-16 15:11:15.202742, what updated=first hash,
http last modified=None,
MD5 hash=be5802368e5a6f7ad172f27732001f3a, hash last modified=None, when checked=2017-12-18 16:03:33.208327,
api=False, error=None)>"""
            )
            dbresource = (
                dbsession.query(DBResource)
                .filter(
                    DBResource.run_number == 1,
                    DBResource.id == "7b82976a-ae81-4cef-a76f-12ba14152086",
                )
                .first()
            )
            assert (
                str(dbresource)
                == """<Resource(run number=1, id=7b82976a-ae81-4cef-a76f-12ba14152086, name=Guinea, Liberia, Mali and Sierra Leone Health Facilities, dataset id=ce876595-1263-4df6-a8ca-459f92c532e4,
url=https://docs.google.com/a/megginson.com/spreadsheets/d/1paoIpHiYo7dy_dnf_luUSfowWDwNAWwS3z4GHL2J7Rc/export?format=xlsx&id=1paoIpHiYo7dy_dnf_luUSfowWDwNAWwS3z4GHL2J7Rc,
last modified=2017-12-18 22:21:26.783801, metadata modified=2017-12-18 22:21:26.783801,
latest of modifieds=2017-12-19 10:53:28.606889, what updated=hash,
http last modified=None,
MD5 hash=789, hash last modified=2017-12-19 10:53:28.606889, when checked=2017-12-19 10:53:28.606889,
api=False, error=None)>"""
            )
            count = (
                dbsession.query(DBResource)
                .filter(DBResource.url.like("%data.humdata.org%"))
                .count()
            )
            assert count == 112
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=1, what_updated="filestore", error=None)
                .count()
            )
            assert count == 0
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=1, what_updated="first hash", error=None)
                .count()
            )
            assert count == 4
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=1, what_updated="hash", error=None)
                .count()
            )
            assert count == 3
            count = (
                dbsession.query(DBResource)
                .filter_by(
                    run_number=1, what_updated="http header", error=None
                )
                .count()
            )
            assert count == 0
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=1, api=True)
                .count()
            )
            assert count == 3
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=1, what_updated="internal-nothing")
                .filter(DBResource.error.isnot(None))
                .count()
            )
            assert count == 0
            # select what_updated, api from dbresources where run_number=0 and md5_hash is not null and id in (select id from dbresources where run_number=1 and what_updated like '%hash%');
            hash_updated = (
                dbsession.query(DBResource.id)
                .filter_by(run_number=1)
                .filter(DBResource.what_updated.like("%hash%"))
            )
            assert hash_updated.count() == 8
            count = (
                dbsession.query(DBResource)
                .filter_by(run_number=0)
                .filter(DBResource.md5_hash.isnot(None))
                .filter(DBResource.id.in_(hash_updated.as_scalar()))
                .count()
            )
            assert count == 4
            dbdataset = dbsession.query(DBDataset).first()
            assert (
                str(dbdataset)
                == """<Dataset(run number=0, id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, dataset date=09/21/2017, update frequency=1,
review date=None, last modified=2017-12-16 15:11:15.204215, metadata modified=2017-12-16 15:11:15.204215, updated by script=None,
latest of modifieds=2017-12-16 15:11:15.204215, what updated=firstrun,
Resource b21d6004-06b5-41e5-8e3e-0f28140bff64: last modified=2017-12-16 15:11:15.202742,
Dataset fresh=2, error=False)>"""
            )
            count = (
                dbsession.query(DBDataset)
                .filter_by(run_number=1, fresh=0, what_updated="filestore")
                .count()
            )
            assert count == 4
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1, fresh=0, what_updated="nothing", error=False
                )
                .count()
            )
            assert count == 59
            count = (
                dbsession.query(DBDataset)
                .filter_by(run_number=1, fresh=0, what_updated="review date")
                .count()
            )
            assert count == 1
            count = (
                dbsession.query(DBDataset)
                .filter_by(run_number=1, fresh=0, what_updated="script update")
                .count()
            )
            assert count == 1
            count = (
                dbsession.query(DBDataset)
                .filter_by(run_number=1, fresh=1, what_updated="nothing")
                .count()
            )
            assert count == 1
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1, fresh=2, what_updated="nothing", error=False
                )
                .count()
            )
            assert count == 1
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1, fresh=3, what_updated="nothing", error=False
                )
                .count()
            )
            assert count == 23
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1, fresh=3, what_updated="nothing", error=True
                )
                .count()
            )
            assert count == 4
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1,
                    fresh=None,
                    what_updated="nothing",
                    error=False,
                )
                .count()
            )
            assert count == 4
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1,
                    fresh=None,
                    what_updated="nothing",
                    error=True,
                )
                .count()
            )
            assert count == 0
            count = (
                dbsession.query(DBDataset)
                .filter_by(
                    run_number=1,
                    fresh=None,
                    what_updated="no resources",
                    error=True,
                )
                .count()
            )
            assert count == 1
            dbinfodataset = dbsession.query(DBInfoDataset).first()
            assert (
                str(dbinfodataset)
                == """<InfoDataset(id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, name=rohingya-displacement-topline-figures, title=Rohingya Displacement Topline Figures,
private=False, organization id=hdx,
maintainer=7d7f5f8d-7e3b-483a-8de1-2b122010c1eb, location=bgd)>"""
            )
            count = dbsession.query(DBInfoDataset).count()
            assert count == 104
            dborganization = dbsession.query(DBOrganization).first()
            assert (
                str(dborganization)
                == """<Organization(id=hdx, name=hdx, title=HDX)>"""
            )
            count = dbsession.query(DBOrganization).count()
            assert count == 40

            assert freshness.resource_last_modified_count == 2

            freshness.previous_run_number = freshness.run_number
            assert freshness.no_resources_force_hash() == 600
