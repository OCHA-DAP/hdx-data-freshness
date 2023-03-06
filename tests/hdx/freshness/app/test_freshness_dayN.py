"""
Unit tests for the freshness class.

Takes base database from fixtures/day0/test_freshness.db

Run data comes from fixtures/dayN/test_serialize.db

When adding new test data, remember that self.now (run date) is 2017-12-19 10:53:28.606889

Set force_hash in dbtestresults and dbtesthashresults if dataset is fresh or you'll get duplicates
(number of resources won't sum to total)!
"""
from os import remove
from os.path import join
from shutil import copyfile

import pytest
from hdx.database import Database
from hdx.utilities.dateparse import parse_date
from sqlalchemy import func, select

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
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "day0", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

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
        return serializedbsession.scalars(
            select(DBTestResult.id).where(DBTestResult.force_hash == 1)
        ).all()

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
            modified = parse_date(
                "2015-05-07 14:44:56.599079", include_microseconds=True
            )
            hash_modified = parse_date(
                "2017-12-16 16:03:33.208327", include_microseconds=True
            )
            dbresource = DBResource(
                run_number=-1,
                id="010ab2d2-8f98-409b-a1f0-4707ad6c040a",
                name="sidih_190.csv",
                dataset_id="54d6b4b8-8cc9-42d3-82ce-3fa4fd3d9be1",
                url="https://ds-ec2.scraperwiki.com/egzfk1p/siqsxsgjnxgk3r2/cgi-bin/csv/sidih_190.csv",
                last_modified=modified,
                metadata_modified=modified,
                latest_of_modifieds=modified,
                what_updated="",
                http_last_modified=None,
                md5_hash="999",
                hash_last_modified=hash_modified,
                when_checked=hash_modified,
                api=False,
                error=None,
            )
            dbsession.add(dbresource)
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

            dbrun = dbsession.execute(
                select(DBRun).where(DBRun.run_number == 1)
            ).scalar_one()
            assert (
                str(dbrun)
                == "<Run number=1, Run date=2017-12-19 10:53:28.606889+00:00>"
            )
            dbresource = dbsession.scalar(select(DBResource).limit(1))
            assert (
                str(dbresource)
                == """<Resource(run number=0, id=b21d6004-06b5-41e5-8e3e-0f28140bff64, name=Topline Numbers.csv, dataset id=a2150ad9-2b87-49f5-a6b2-c85dff366b75,
url=https://docs.google.com/spreadsheets/d/e/2PACX-1vRjFRZGLB8IMp0anSGR1tcGxwJgkyx0bTN9PsinqtaLWKHBEfz77LkinXeVqIE_TsGVt-xM6DQzXpkJ/pub?gid=0&single=true&output=csv,
last modified=2017-12-16 15:11:15.202742+00:00, metadata modified=2017-12-16 15:11:15.202742+00:00,
latest of modifieds=2017-12-16 15:11:15.202742+00:00, what updated=first hash,
http last modified=None,
MD5 hash=be5802368e5a6f7ad172f27732001f3a, hash last modified=None, when checked=2017-12-18 16:03:33.208327+00:00,
api=False, error=None)>"""
            )
            dbresource = dbsession.scalar(
                select(DBResource)
                .where(
                    DBResource.run_number == 1,
                    DBResource.id == "7b82976a-ae81-4cef-a76f-12ba14152086",
                )
                .limit(1)
            )
            assert (
                str(dbresource)
                == """<Resource(run number=1, id=7b82976a-ae81-4cef-a76f-12ba14152086, name=Guinea, Liberia, Mali and Sierra Leone Health Facilities, dataset id=ce876595-1263-4df6-a8ca-459f92c532e4,
url=https://docs.google.com/a/megginson.com/spreadsheets/d/1paoIpHiYo7dy_dnf_luUSfowWDwNAWwS3z4GHL2J7Rc/export?format=xlsx&id=1paoIpHiYo7dy_dnf_luUSfowWDwNAWwS3z4GHL2J7Rc,
last modified=2017-12-18 22:21:26.783801+00:00, metadata modified=2017-12-18 22:21:26.783801+00:00,
latest of modifieds=2017-12-19 10:53:28.606889+00:00, what updated=hash,
http last modified=None,
MD5 hash=789, hash last modified=2017-12-19 10:53:28.606889+00:00, when checked=2017-12-19 10:53:28.606889+00:00,
api=False, error=None)>"""
            )
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.url.like("%data.humdata.org%")
                )
            )
            assert count == 112
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated == "filestore",
                    DBResource.error.is_(None),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated == "first hash",
                    DBResource.error.is_(None),
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated == "hash",
                    DBResource.error.is_(None),
                )
            )
            assert count == 3
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated == "http header",
                    DBResource.error.is_(None),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1, DBResource.api.is_(True)
                )
            )
            assert count == 3
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated == "internal-nothing",
                    DBResource.error.is_not(None),
                )
            )
            assert count == 0
            # select what_updated, api from dbresources where run_number=0 and md5_hash is not null and id in (select id from dbresources where run_number=1 and what_updated like '%hash%');
            hash_updated = dbsession.scalars(
                select(DBResource.id).where(
                    DBResource.run_number == 1,
                    DBResource.what_updated.like("%hash%"),
                )
            ).all()
            assert len(hash_updated) == 8
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.run_number == 0,
                    DBResource.md5_hash.is_not(None),
                    DBResource.id.in_(hash_updated),
                )
            )
            assert count == 4
            dbdataset = dbsession.scalar(select(DBDataset).limit(1))
            assert (
                str(dbdataset)
                == """<Dataset(run number=0, id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, reference period=09/21/2017, update frequency=1,
review date=None, last modified=2017-12-16 15:11:15.204215+00:00, metadata modified=2017-12-16 15:11:15.204215+00:00, updated by script=None,
latest of modifieds=2017-12-16 15:11:15.204215+00:00, what updated=firstrun,
Resource b21d6004-06b5-41e5-8e3e-0f28140bff64: last modified=2017-12-16 15:11:15.202742+00:00,
Dataset fresh=2, error=False)>"""
            )
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "filestore",
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 59
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "review date",
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "script update",
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 1,
                    DBDataset.what_updated == "nothing",
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 2,
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 3,
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 23
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh == 3,
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh.is_(None),
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh.is_(None),
                    DBDataset.what_updated == "nothing",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.run_number == 1,
                    DBDataset.fresh.is_(None),
                    DBDataset.what_updated == "no resources",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 1
            dbinfodataset = dbsession.scalar(select(DBInfoDataset).limit(1))
            assert (
                str(dbinfodataset)
                == """<InfoDataset(id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, name=rohingya-displacement-topline-figures, title=Rohingya Displacement Topline Figures,
private=False, organization id=hdx,
maintainer=7d7f5f8d-7e3b-483a-8de1-2b122010c1eb, location=bgd)>"""
            )
            count = dbsession.scalar(select(func.count(DBInfoDataset.id)))
            assert count == 104
            dborganization = dbsession.scalar(select(DBOrganization).limit(1))
            assert (
                str(dborganization)
                == """<Organization(id=hdx, name=hdx, title=HDX)>"""
            )
            count = dbsession.scalar(select(func.count(DBOrganization.id)))
            assert count == 40

            assert freshness.resource_last_modified_count == 2

            freshness.previous_run_number = freshness.run_number
            assert freshness.no_resources_force_hash() == 600
