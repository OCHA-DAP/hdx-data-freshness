"""
Unit tests for process_results.

"""
import datetime
from unittest.mock import Mock

import pytest
from dateutil import parser
from hdx.data.dataset import Dataset

from hdx.freshness.app.datafreshness import DataFreshness


class TestProcessResults:
    @pytest.fixture(scope="function")
    def session(self):
        class TestSession:
            @staticmethod
            def query(somethingin):
                something = str(somethingin)
                result = Mock()
                result.scalar.return_value = False
                result.distinct.return_value.order_by.return_value.first.return_value = (
                    None
                )

                if "DBResource" in something:

                    class DBResource:
                        dataset_id = "c1c85ecb-5e84-48c6-8ba9-15689a6c2fc4"
                        what_updated = ""
                        http_last_modified = None
                        latest_of_modifieds = parser.parse(
                            "2019-10-28 05:05:20"
                        )
                        md5_hash = "5600bafa19852afae3d7fd27955df0e6"
                        error = ""

                    result.filter_by.return_value.one.return_value = (
                        DBResource()
                    )
                else:

                    class DBDataset:
                        fresh = 0
                        update_frequency = 7

                    result.filter_by.return_value.one.return_value = (
                        DBDataset()
                    )
                return result

            @staticmethod
            def commit():
                pass

        return TestSession()

    @pytest.fixture(scope="function")
    def now(self):
        return parser.parse("2019-11-03 23:01:31.438713")

    @pytest.fixture(scope="function")
    def datasets(self):
        ds = {
            "title": "HOTOSM Afghanistan Points of Interest (OpenStreetMap Export)",
            "package_creator": "osm2hdx",
            "data_update_frequency": "7",
            "maintainer": "6a0688ce-8521-46e2-8edd-8e26c0851ebd",
            "private": False,
            "dataset_date": "11/03/2019",
            "id": "c1c85ecb-5e84-48c6-8ba9-15689a6c2fc4",
            "metadata_created": "2019-08-16T00:35:57.276354",
            "metadata_modified": "2019-11-04T08:45:39.230224",
            "subnational": "1",
            "methodology": "Other",
            "license_id": "hdx-odc-odbl",
            "dataset_source": "OpenStreetMap contributors",
            "tags": [
                {
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                    "name": "facilities and infrastructure",
                },
                {
                    "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                    "name": "points of interest - poi",
                },
            ],
            "last_modified": "2019-08-16T00:48:44.310234",
            "groups": [{"name": "afg"}],
            "methodology_other": "Volunteered geographic information",
            "name": "hotosm_afg_points_of_interest",
            "notes": "OpenStreetMap",
            "owner_org": "225b9f7d-e7cb-4156-96a6-44c9c58d31e3",
            "resources": [
                {
                    "id": "fe295e74-56d6-4a83-b50c-43001a204b0f",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": "ESRI Shapefile",
                    "format": "SHP",
                    "last_modified": "2019-08-16T00:48:38.609336",
                    "name": "hotosm_afg_points_of_interest_points_shp.zip",
                    "created": "2019-08-16T00:48:38.783589",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_points_shp.zip",
                },
                {
                    "id": "bd64f67f-aa13-479d-85a6-c05f036a210c",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": "ESRI Shapefile",
                    "format": "SHP",
                    "last_modified": "2019-08-16T00:48:38.609336",
                    "name": "hotosm_afg_points_of_interest_polygons_shp.zip",
                    "created": "2019-08-16T00:48:38.783599",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_polygons_shp.zip",
                },
                {
                    "id": "dc8be6ea-d9d5-48e0-afe1-7e5f7e6be652",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": "Geopackage, SQLite compatible",
                    "format": "Geopackage",
                    "last_modified": "2019-08-16T00:48:44.310234",
                    "name": "hotosm_afg_points_of_interest_gpkg.zip",
                    "created": "2019-08-16T00:48:38.783604",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_gpkg.zip",
                },
                {
                    "id": "5df64cf7-d782-47f2-99a4-5258f1389033",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": "Google Earth .KML",
                    "format": "KML",
                    "last_modified": "2019-08-16T00:48:44.310234",
                    "name": "hotosm_afg_points_of_interest_points_kml.zip",
                    "created": "2019-08-16T00:48:38.783608",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_points_kml.zip",
                },
                {
                    "id": "3adb573a-f056-41b7-8ee5-ec245676a7ce",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": "Google Earth .KML",
                    "format": "KML",
                    "last_modified": "2019-08-16T00:48:44.310234",
                    "name": "hotosm_afg_points_of_interest_polygons_kml.zip",
                    "created": "2019-08-16T00:48:38.783613",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_polygons_kml.zip",
                },
                {
                    "id": "5cf4261f-b571-4bf4-9a5c-2998f49be722",
                    "metadata_modified": "2019-11-04T08:45:39.226896",
                    "description": ".IMG for Garmin GPS Devices (All OSM layers for area)",
                    "format": "ERDAS Image",
                    "last_modified": "2019-08-16T00:48:44.310234",
                    "name": "hotosm_afg_gmapsupp_img.zip",
                    "created": "2019-08-16T00:48:38.783617",
                    "url": "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_gmapsupp_img.zip",
                },
            ],
        }
        return [Dataset(ds)]

    @pytest.fixture(scope="function")
    def results(self):
        results = {
            "3adb573a-f056-41b7-8ee5-ec245676a7ce": (
                "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_polygons_kml.zip",
                "application/zip",
                None,
                parser.parse("2019-11-03 14:23:40"),
                "33caf1b1106613d123989c2b459c383d",
                None,
            )
        }
        return results

    @pytest.fixture(scope="function")
    def error1(self):
        return "code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=http://export.hotosm.org/downloads/64fa006f-4781-4d07-a645-ed0ea9d047e7/hotosm_uga_rr_railways_points_kml.zip"

    @pytest.fixture(scope="function")
    def error2(self):
        return "code= message=Timeout on reading data from socket raised=aiohttp.client_exceptions.ServerTimeoutError url=https://fdw.fews.net/api/marketpricefacts/?dataset=FEWS_NET_Staple_Food_Price_Data&country=NG&format=csv&fields=website&end_date"

    @pytest.fixture(scope="function")
    def broken_results1(self, error1):
        results = {
            "5cf4261f-b571-4bf4-9a5c-2998f49be722": (
                "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_polygons_kml.zip",
                "application/zip",
                error1,
                None,
                None,
                None,
            )
        }
        return results

    @pytest.fixture(scope="function")
    def broken_results2(self, error2):
        results = {
            "5cf4261f-b571-4bf4-9a5c-2998f49be722": (
                "http://export.hotosm.org/downloads/1364e367-304e-4df2-989c-839760c3728d/hotosm_afg_points_of_interest_polygons_kml.zip",
                "application/zip",
                error2,
                None,
                None,
                None,
            )
        }
        return results

    def test_process_results(
        self, configuration, session, now, datasets, results, resourcecls
    ):
        freshness = DataFreshness(
            session=session, datasets=datasets, now=now, do_touch=True
        )
        resourcecls.populate_resourcedict(datasets)
        resourcecls.touched = False
        datasets_lastmodified = freshness.process_results(
            results, results, resourcecls=resourcecls
        )
        assert datasets_lastmodified == {
            "c1c85ecb-5e84-48c6-8ba9-15689a6c2fc4": {
                "3adb573a-f056-41b7-8ee5-ec245676a7ce": (
                    "",
                    datetime.datetime(2019, 11, 3, 23, 1, 31, 438713),
                    ",hash",
                )
            }
        }
        assert resourcecls.touched is True

    def test_process_broken_results1(
        self,
        configuration,
        session,
        now,
        datasets,
        error1,
        broken_results1,
        resourcecls,
    ):
        freshness = DataFreshness(
            session=session, datasets=datasets, now=now, do_touch=True
        )
        resourcecls.populate_resourcedict(datasets)
        resourcecls.broken = False
        datasets_lastmodified = freshness.process_results(
            broken_results1, broken_results1, resourcecls=resourcecls
        )
        assert datasets_lastmodified == {
            "c1c85ecb-5e84-48c6-8ba9-15689a6c2fc4": {
                "5cf4261f-b571-4bf4-9a5c-2998f49be722": (
                    error1,
                    datetime.datetime(2019, 10, 28, 5, 5, 20),
                    "",
                )
            }
        }
        assert resourcecls.broken is True

    def test_process_broken_results2(
        self,
        configuration,
        session,
        now,
        datasets,
        error2,
        broken_results2,
        resourcecls,
    ):
        freshness = DataFreshness(
            session=session, datasets=datasets, now=now, do_touch=True
        )
        resourcecls.populate_resourcedict(datasets)
        resourcecls.broken = False
        datasets_lastmodified = freshness.process_results(
            broken_results2, broken_results2, resourcecls=resourcecls
        )
        assert datasets_lastmodified == {
            "c1c85ecb-5e84-48c6-8ba9-15689a6c2fc4": {
                "5cf4261f-b571-4bf4-9a5c-2998f49be722": (
                    error2,
                    datetime.datetime(2019, 10, 28, 5, 5, 20),
                    "",
                )
            }
        }
        assert resourcecls.broken is False
