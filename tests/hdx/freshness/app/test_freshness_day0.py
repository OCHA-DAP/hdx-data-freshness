"""
Unit tests for the freshness class.

"""
from os import remove
from os.path import join

import pytest
from hdx.database import Database
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


class TestFreshnessDay0:
    @pytest.fixture(scope="function")
    def nodatabase(self):
        dbpath = join("tests", "test_freshness.db")
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="class")
    def serializedbsession(self):
        dbpath = join("tests", "fixtures", "day0", "test_serialize.db")
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
        nodatabase,
        now,
        datasets,
        results,
        hash_results,
        forced_hash_ids,
        resourcecls,
    ):
        with Database(**nodatabase) as session:
            freshness = DataFreshness(
                session=session, datasets=datasets, now=now
            )
            freshness.spread_datasets()
            freshness.add_new_run()
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
            assert (
                output
                == """
*** Resources ***
* total: 660 *,
api: 4,
error: 27,
first hash: 9,
firstrun: 564,
internal-firstrun: 56

*** Datasets ***
* total: 103 *,
0: Fresh, Updated firstrun: 67,
1: Due, Updated firstrun: 1,
2: Overdue, Updated firstrun: 1,
3: Delinquent, Updated firstrun: 25,
3: Delinquent, Updated firstrun,error: 5,
Freshness Unavailable, Updated firstrun: 4

15 datasets have update frequency of Live
19 datasets have update frequency of Never
0 datasets have update frequency of As Needed"""
            )

            dbsession = freshness.session
            dbrun = dbsession.execute(select(DBRun)).scalar_one()
            assert (
                str(dbrun)
                == "<Run number=0, Run date=2017-12-18 16:03:33.208327+00:00>"
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
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.url.like("%data.humdata.org%")
                )
            )
            assert count == 56
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "internal-firstrun",
                    DBResource.error.is_(None),
                    DBResource.api.is_(None),
                )
            )
            assert count == 56
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "internal-firstrun,hash",
                    DBResource.error.is_(None),
                    DBResource.api.is_(False),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated
                    == "internal-firstrun,http header,hash",
                    DBResource.error.is_(None),
                    DBResource.api.is_(False),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "firstrun",
                    DBResource.error.is_(None),
                    DBResource.api.is_(None),
                )
            )
            assert count == 564
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "firstrun",
                    DBResource.error.is_(None),
                    DBResource.api.is_(True),
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.error.is_not(None),
                    DBResource.what_updated == "firstrun",
                )
            )
            assert count == 27
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "first hash",
                    DBResource.error.is_(None),
                    DBResource.api.is_(False),
                )
            )
            assert count == 9
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "http header",
                    DBResource.error.is_(None),
                    DBResource.api.is_(None),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBResource.id)).where(
                    DBResource.what_updated == "http header,hash",
                    DBResource.error.is_(None),
                    DBResource.api.is_(False),
                )
            )
            assert count == 0
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
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 67
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "http header",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 0,
                    DBDataset.what_updated == "http header",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 1, DBDataset.what_updated == "firstrun"
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 2,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 1
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 2,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 3,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 25
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 3,
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 5
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh == 3,
                    DBDataset.what_updated == "http header",
                )
            )
            assert count == 0
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh.is_(None),
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(False),
                )
            )
            assert count == 4
            count = dbsession.scalar(
                select(func.count(DBDataset.id)).where(
                    DBDataset.fresh.is_(None),
                    DBDataset.what_updated == "firstrun",
                    DBDataset.error.is_(True),
                )
            )
            assert count == 0
            dbinfodataset = dbsession.scalar(select(DBInfoDataset).limit(1))
            assert (
                str(dbinfodataset)
                == """<InfoDataset(id=a2150ad9-2b87-49f5-a6b2-c85dff366b75, name=rohingya-displacement-topline-figures, title=Rohingya Displacement Topline Figures,
private=False, organization id=hdx,
maintainer=7d7f5f8d-7e3b-483a-8de1-2b122010c1eb, location=bgd)>"""
            )
            count = dbsession.scalar(select(func.count(DBInfoDataset.id)))
            assert count == 103
            dborganization = dbsession.scalar(select(DBOrganization).limit(1))
            assert (
                str(dborganization)
                == """<Organization(id=hdx, name=hdx, title=HDX)>"""
            )
            count = dbsession.scalar(select(func.count(DBOrganization.id)))
            assert count == 40
