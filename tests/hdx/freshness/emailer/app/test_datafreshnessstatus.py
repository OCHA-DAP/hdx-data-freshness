"""
Unit tests for the data freshness status code.

"""

import copy
from os import getenv, remove
from os.path import join
from shutil import copyfile

import pytest

from hdx.data.dataset import Dataset
from hdx.database import Database
from hdx.freshness.database import Base
from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.emailer.app.datafreshnessstatus import DataFreshnessStatus
from hdx.freshness.emailer.utils.databasequeries import DatabaseQueries
from hdx.freshness.emailer.utils.freshnessemail import Email
from hdx.freshness.emailer.utils.hdxhelper import HDXHelper
from hdx.freshness.emailer.utils.sheet import Sheet
from hdx.utilities.dateparse import parse_date


class TestDataFreshnessStatus:
    email_users_result = list()
    cells_result = list()

    @staticmethod
    def email_users(users_to_email, subject, text_body, html_body, cc, bcc):
        TestDataFreshnessStatus.email_users_result.append(
            (users_to_email, subject, text_body, html_body, cc, bcc)
        )

    class TestDataset(Dataset):
        @staticmethod
        def search_in_hdx(fq):
            if "airport" in fq and "groups:wsm" in fq:
                return [{"id": "34cb4297-36e2-40b0-b822-47320ea9314c"}]
            else:
                return list()

    class TestSpreadsheet_Broken1:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Freshness",
                            "Error Type",
                            "Error",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries",
                            "Yemen - Administrative Boundaries",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Every year",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_Broken2:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Freshness",
                            "Error Type",
                            "Error",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ]
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_Broken3:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Freshness",
                            "Error Type",
                            "Error",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-live",
                            "Yemen - Administrative Boundaries Live",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Live",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-day",
                            "Yemen - Administrative Boundaries Day",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Every day",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-week",
                            "Yemen - Administrative Boundaries Week",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Every week",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-month",
                            "Yemen - Administrative Boundaries Month",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Every month",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-as-needed",
                            "Yemen - Administrative Boundaries Needed",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "As needed",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                        [
                            "http://lala/dataset/yemen-admin-boundaries-never",
                            "Yemen - Administrative Boundaries Never",
                            "OCHA Yemen",
                            "",
                            "",
                            "blah4disp,blah5full",
                            "blah4@blah.com,blah5@blah.com",
                            "Never",
                            "2015-12-28T06:39:20.134647",
                            "Delinquent",
                            "ClientConnectorError",
                            "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                            "2017-01-02T19:07:30.333492",
                            "2017-01-02T19:07:30.333492",
                            3,
                            "Andrew",
                            "Contacted Maintainer",
                        ],
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_OverdueDelinquent:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ],
                        [
                            "http://lala/dataset/ourairports-myt",
                            "Airports in Mayotte",
                            "OurAirports",
                            "blah5full",
                            "blah5@blah.com",
                            "blah3disp,blah4disp,blah5full",
                            "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                            "Every year",
                            "2015-11-24T23:32:32.025059",
                            "2017-01-01T19:07:30.333492",
                            "2017-01-01T19:07:30.333492",
                            2,
                            "Peter",
                            "Done",
                        ],
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_MaintainerOrgAdmins:
        @staticmethod
        def worksheet(sheetname):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    if TestWorksheet.sheetname == "Maintainer":
                        return [
                            [
                                "URL",
                                "Title",
                                "Organisation",
                                "Maintainer",
                                "Maintainer Email",
                                "Org Admins",
                                "Org Admin Emails",
                                "Update Frequency",
                                "Latest of Modifieds",
                                "Date Added",
                                "Date Last Occurred",
                                "No. Times",
                                "Assigned",
                                "Status",
                            ],
                            [
                                "http://lala/dataset/ourairports-myt",
                                "Airports in Mayotte",
                                "OurAirports",
                                "blah5full",
                                "blah5@blah.com",
                                "blah3disp,blah4disp,blah5full",
                                "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                                "Every year",
                                "2015-11-24T23:32:32.025059",
                                "2017-01-01T19:07:30.333492",
                                "2017-01-01T19:07:30.333492",
                                3,
                                "Aaron",
                                "Done",
                            ],
                        ]
                    else:
                        return [
                            [
                                "URL",
                                "Title",
                                "Error",
                                "Date Added",
                                "Date Last Occurred",
                                "No. Times",
                                "Assigned",
                                "Status",
                            ],
                            [
                                "http://lala/organization/ocha-somalia",
                                "OCHA Somalia",
                                "All org admins are sysadmins!",
                                "2017-01-01T19:07:30.333492",
                                "2017-01-01T19:07:30.333492",
                                5,
                                "Aaron",
                                "",
                            ],
                        ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result.append(cells)

            TestWorksheet.sheetname = sheetname
            return TestWorksheet

    class TestSpreadsheet_NoResources:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ],
                        [
                            "http://lala/dataset/ourairports-myt",
                            "Airports in Mayotte",
                            "OurAirports",
                            "blah5full",
                            "blah5@blah.com",
                            "blah3disp,blah4disp,blah5full",
                            "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                            "Every year",
                            "2015-11-24T23:32:32.025059",
                            "2017-01-01T19:07:30.333492",
                            "2017-01-01T19:07:30.333492",
                            2,
                            "Peter",
                            "Done",
                        ],
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    class TestSpreadsheet_Empty:
        @staticmethod
        def worksheet(_):
            class TestWorksheet:
                @staticmethod
                def get_values():
                    return [
                        [
                            "URL",
                            "Title",
                            "Organisation",
                            "Maintainer",
                            "Maintainer Email",
                            "Org Admins",
                            "Org Admin Emails",
                            "Dataset Date",
                            "Update Frequency",
                            "Latest of Modifieds",
                            "Date Added",
                            "Date Last Occurred",
                            "No. Times",
                            "Assigned",
                            "Status",
                        ]
                    ]

                @staticmethod
                def clear():
                    return

                @staticmethod
                def update(_, cells):
                    TestDataFreshnessStatus.cells_result = cells

            return TestWorksheet

    @pytest.fixture(scope="class")
    def users(self):
        return [
            {
                "email": "blah@blah.com",
                "id": "blah",
                "name": "blahname",
                "sysadmin": False,
                "fullname": "blahfull",
                "display_name": "blahdisp",
            },
            {
                "email": "blah2@blah.com",
                "id": "blah2",
                "name": "blah2name",
                "sysadmin": True,
                "fullname": "blah2full",
            },
            {
                "email": "blah3@blah.com",
                "id": "blah3",
                "name": "blah3name",
                "sysadmin": True,
                "fullname": "blah3full",
                "display_name": "blah3disp",
            },
            {
                "email": "blah4@blah.com",
                "id": "blah4",
                "name": "blah4name",
                "sysadmin": True,
                "fullname": "blah4full",
                "display_name": "blah4disp",
            },
            {
                "email": "blah5@blah.com",
                "id": "blah5",
                "name": "blah5name",
                "sysadmin": False,
                "fullname": "blah5full",
            },
        ]

    @pytest.fixture(scope="class")
    def organizations(self, users):
        orgusers = list()
        for user in users:
            orguser = {"capacity": "admin"}
            orguser.update(user)
            del orguser["email"]
            orgusers.append(orguser)
        return [
            {
                "display_name": "OCHA Colombia",
                "description": "OCHA Colombia",
                "image_display_url": "",
                "package_count": 147,
                "created": "2014-04-28T17:50:16.250998",
                "name": "ocha-colombia",
                "is_organization": True,
                "state": "active",
                "image_url": "",
                "type": "organization",
                "title": "OCHA Colombia",
                "revision_id": "7b70966b-c614-47e2-99d7-fafce4cbd2fa",
                "num_followers": 0,
                "id": "15942bd7-524a-40d6-8a60-09bd78110d2d",
                "approval_status": "approved",
                "users": [
                    copy.deepcopy(orgusers[2]),
                    copy.deepcopy(orgusers[4]),
                ],
            },
            {
                "display_name": "OCHA Somalia",
                "description": "OCHA Somalia",
                "image_display_url": "",
                "package_count": 27,
                "created": "2014-11-06T17:35:37.390084",
                "name": "ocha-somalia",
                "is_organization": True,
                "state": "active",
                "image_url": "",
                "type": "organization",
                "title": "OCHA Somalia",
                "revision_id": "6eb690cc-7821-45e1-99a0-6094894a04d7",
                "num_followers": 0,
                "id": "68aa2b4d-ea41-4b79-8e37-ac03cbe9ddca",
                "approval_status": "approved",
                "users": [
                    copy.deepcopy(orgusers[2]),
                    copy.deepcopy(orgusers[3]),
                ],
            },
            {
                "display_name": "OCHA Yemen",
                "description": "OCHA Yemen.",
                "image_display_url": "",
                "package_count": 40,
                "created": "2014-04-28T17:47:48.530487",
                "name": "ocha-yemen",
                "is_organization": True,
                "state": "active",
                "image_url": "",
                "type": "organization",
                "title": "OCHA Yemen",
                "revision_id": "d0f65677-e8ef-46a5-a28f-6c8ab3acf05e",
                "num_followers": 0,
                "id": "cdcb3c1f-b8d5-4154-a356-c7021bb1ffbd",
                "approval_status": "approved",
                "users": [
                    copy.deepcopy(orgusers[3]),
                    copy.deepcopy(orgusers[4]),
                ],
            },
            {
                "display_name": "OurAirports",
                "description": "http://ourairports.com",
                "image_display_url": "",
                "package_count": 238,
                "created": "2014-04-24T22:00:54.948536",
                "name": "ourairports",
                "is_organization": True,
                "state": "active",
                "image_url": "",
                "type": "organization",
                "title": "OurAirports",
                "revision_id": "720eae06-2877-4d13-8af4-30061f6a72a5",
                "num_followers": 0,
                "id": "dc65f72e-ba98-40aa-ad32-bec0ed1e10a2",
                "approval_status": "approved",
                "users": [
                    copy.deepcopy(orgusers[2]),
                    copy.deepcopy(orgusers[3]),
                    copy.deepcopy(orgusers[4]),
                ],
            },
        ]

    @pytest.fixture(scope="function")
    def database_broken(self):
        dbfile = "test_freshness_broken.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="function")
    def database_status(self):
        dbfile = "test_freshness_status.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="function")
    def database_maintainer(self):
        dbfile = "test_freshness_maintainer.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="function")
    def database_noresources(self):
        dbfile = "test_freshness_noresources.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="function")
    def database_datasets_modified_yesterday(self):
        dbfile = "test_freshness_modified_yesterday.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    def test_freshnessbroken(
        self, configuration, database_broken, users, organizations
    ):
        site_url = "http://lala"
        now = parse_date("2017-02-03 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        sysadmin_emails = ["blah3@blah.com", "blah4@blah.com"]
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=self.email_users,
        )
        with Database(**database_broken, table_base=Base) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )

            sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken1
            sheet.dutyofficer = {"name": "Peter", "email": "peter@lala.org"}
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["peter@lala.org"],
                    "Broken datasets (03/02/2017)",
                    "Dear Peter,\n\nThe following datasets have broken resources:\n\nClientConnectorError\nOCHA Somalia\n    Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!\n\nClientConnectorSSLError\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n        Resource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n\nClientResponseError\nOCHA Colombia\n    Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n\nFormat Mismatch\nOurAirports\n    Airports in Somewhere (http://lala/dataset/ourairports-som) with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Overdue\n        Resource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!\n\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Peter,<br><br>The following datasets have broken resources:<br><br><b>ClientConnectorError</b><br><b><i>OCHA Somalia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!<br><br><b>ClientConnectorSSLError</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br><br><b>ClientResponseError</b><br><b><i>OCHA Colombia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><br><b>Format Mismatch</b><br><b><i>OurAirports</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-som">Airports in Somewhere</a> with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Overdue<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!<br><br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah3@blah.com", "blah4@blah.com"],
                    None,
                )
            ]
            assert TestDataFreshnessStatus.cells_result == [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Freshness",
                    "Error Type",
                    "Error",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries",
                    "Yemen - Administrative Boundaries",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientResponseError",
                    "Admin-0.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx\nAdmin-3.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx",
                    "2017-01-02T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    4,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/projected-ipc-population-estimates-february-june-2016",
                    "Projected IPC population Estimates February - June 2016",
                    "OCHA Somalia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah4disp",
                    "blah3@blah.com,blah4@blah.com",
                    "Every six months",
                    "2016-07-17T10:13:34.099517",
                    "Delinquent",
                    "ClientConnectorError",
                    "Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx:error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015",
                    "Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015",
                    "OCHA Colombia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah5full",
                    "blah3@blah.com,blah5@blah.com",
                    "Every six months",
                    "2016-07-17T10:25:57.626518",
                    "Delinquent",
                    "ClientResponseError",
                    "160304Tendencias_Humanitarias_2016_I.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx\n160304Tendencias_Humanitarias_2016_I_2.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/ourairports-som",
                    "Airports in Somewhere",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:36:47.280228",
                    "Overdue",
                    "Format Mismatch",
                    "List of airports in Somewhere:File mimetype html does not match HDX format text/csv!",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
            ]

            sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken2
            sheet.dutyofficer = {"name": "John", "email": "john@lala.org"}
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken(recipients=["alex@lala.org", "jenny@lala.org"])
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["alex@lala.org", "jenny@lala.org"],
                    "Broken datasets (03/02/2017)",
                    "Dear system administrator,\n\nThe following datasets have broken resources:\n\nClientConnectorError\nOCHA Somalia\n    Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!\n\nClientConnectorSSLError\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n        Resource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n\nClientResponseError\nOCHA Colombia\n    Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n\nFormat Mismatch\nOurAirports\n    Airports in Somewhere (http://lala/dataset/ourairports-som) with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Overdue\n        Resource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!\n\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>The following datasets have broken resources:<br><br><b>ClientConnectorError</b><br><b><i>OCHA Somalia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!<br><br><b>ClientConnectorSSLError</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br><br><b>ClientResponseError</b><br><b><i>OCHA Colombia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><br><b>Format Mismatch</b><br><b><i>OurAirports</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-som">Airports in Somewhere</a> with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Overdue<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!<br><br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                )
            ]
            assert TestDataFreshnessStatus.cells_result == [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Freshness",
                    "Error Type",
                    "Error",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/projected-ipc-population-estimates-february-june-2016",
                    "Projected IPC population Estimates February - June 2016",
                    "OCHA Somalia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah4disp",
                    "blah3@blah.com,blah4@blah.com",
                    "Every six months",
                    "2016-07-17T10:13:34.099517",
                    "Delinquent",
                    "ClientConnectorError",
                    "Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx:error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "John",
                    "",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries",
                    "Yemen - Administrative Boundaries",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientResponseError",
                    "Admin-0.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx\nAdmin-3.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "John",
                    "",
                ],
                [
                    "http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015",
                    "Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015",
                    "OCHA Colombia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah5full",
                    "blah3@blah.com,blah5@blah.com",
                    "Every six months",
                    "2016-07-17T10:25:57.626518",
                    "Delinquent",
                    "ClientResponseError",
                    "160304Tendencias_Humanitarias_2016_I.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx\n160304Tendencias_Humanitarias_2016_I_2.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "John",
                    "",
                ],
                [
                    "http://lala/dataset/ourairports-som",
                    "Airports in Somewhere",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:36:47.280228",
                    "Overdue",
                    "Format Mismatch",
                    "List of airports in Somewhere:File mimetype html does not match HDX format text/csv!",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "John",
                    "",
                ],
            ]

            sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken3
            sheet.dutyofficer = {"name": "Peter", "email": "peter@lala.org"}
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["peter@lala.org"],
                    "Broken datasets (03/02/2017)",
                    "Dear Peter,\n\nThe following datasets have broken resources:\n\nClientConnectorError\nOCHA Somalia\n    Projected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!\n\nClientConnectorSSLError\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n        Resource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!\n\nClientResponseError\nOCHA Colombia\n    Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months and freshness: Delinquent\n        Resource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\nOCHA Yemen\n    Yemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Delinquent\n        Resource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n        Resource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!\n\nFormat Mismatch\nOurAirports\n    Airports in Somewhere (http://lala/dataset/ourairports-som) with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year and freshness: Overdue\n        Resource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!\n\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Peter,<br><br>The following datasets have broken resources:<br><br><b>ClientConnectorError</b><br><b><i>OCHA Somalia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx (93a361c6-ace7-4cea-8407-ffd2c30d0853) has error: error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx!<br><br><b>ClientConnectorSSLError</b><br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-1.zip (69146e1e-62c7-4e7f-8f6c-2dacffe02283) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-2.zip (f60035dc-624a-49cf-95de-9d489c07d3b9) has error: error: code= message=Cannot connect to host xxx ssl:True [[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed (_ssl.c:645)] raised=aiohttp.client_exceptions.ClientConnectorSSLError url=xxx!<br><br><b>ClientResponseError</b><br><b><i>OCHA Colombia</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I.xlsx (256d8b17-5975-4be6-8985-5df18dda061e) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource 160304Tendencias_Humanitarias_2016_I_2.xlsx (256d8b17-5975-4be6-8985-5df18dda061a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><b><i>OCHA Yemen</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Delinquent<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-0.zip (2ade2886-2990-41d0-a89b-33c5d1de6e3a) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource Admin-3.zip (f60035dc-624a-49cf-95de-9d489c07d3ba) has error: code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx!<br><br><b>Format Mismatch</b><br><b><i>OurAirports</i></b><br>&nbsp&nbsp&nbsp&nbsp<a href="http://lala/dataset/ourairports-som">Airports in Somewhere</a> with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year and freshness: Overdue<br>&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspResource List of airports in Somewhere (89b35e5b-32ea-4470-a854-95e47fe1a95a) has error: File mimetype html does not match HDX format text/csv!!<br><br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah3@blah.com", "blah4@blah.com"],
                    None,
                )
            ]
            result = [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Freshness",
                    "Error Type",
                    "Error",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/projected-ipc-population-estimates-february-june-2016",
                    "Projected IPC population Estimates February - June 2016",
                    "OCHA Somalia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah4disp",
                    "blah3@blah.com,blah4@blah.com",
                    "Every six months",
                    "2016-07-17T10:13:34.099517",
                    "Delinquent",
                    "ClientConnectorError",
                    "Rural-Urban-and-IDP-Projected-Population-February-June-2016[1].xlsx:error: code= message=Cannot connect to host xxx ssl:False [Connection refused] raised=aiohttp.client_exceptions.ClientConnectorError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries",
                    "Yemen - Administrative Boundaries",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientResponseError",
                    "Admin-0.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx\nAdmin-3.zip:code=404 message=Non-retryable response code "
                    "raised=aiohttp.ClientResponseError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015",
                    "Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015",
                    "OCHA Colombia",
                    "blahdisp",
                    "blah@blah.com",
                    "blah3disp,blah5full",
                    "blah3@blah.com,blah5@blah.com",
                    "Every six months",
                    "2016-07-17T10:25:57.626518",
                    "Delinquent",
                    "ClientResponseError",
                    "160304Tendencias_Humanitarias_2016_I.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx\n160304Tendencias_Humanitarias_2016_I_2.xlsx:code=404 message=Non-retryable response code raised=aiohttp.ClientResponseError url=xxx",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/ourairports-som",
                    "Airports in Somewhere",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:36:47.280228",
                    "Overdue",
                    "Format Mismatch",
                    "List of airports in Somewhere:File mimetype html does not match HDX format text/csv!",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Peter",
                    "",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-never",
                    "Yemen - Administrative Boundaries Never",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Never",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-as-needed",
                    "Yemen - Administrative Boundaries Needed",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "As needed",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-month",
                    "Yemen - Administrative Boundaries Month",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every month",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-week",
                    "Yemen - Administrative Boundaries Week",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every week",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-day",
                    "Yemen - Administrative Boundaries Day",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Every day",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
                [
                    "http://lala/dataset/yemen-admin-boundaries-live",
                    "Yemen - Administrative Boundaries Live",
                    "OCHA Yemen",
                    "",
                    "",
                    "blah4disp,blah5full",
                    "blah4@blah.com,blah5@blah.com",
                    "Live",
                    "2015-12-28T06:39:20.134647",
                    "Delinquent",
                    "ClientConnectorError",
                    "Admin-0.zip:Fail\nAdmin-3.zip:Fail",
                    "2017-01-02T19:07:30.333492",
                    "2017-01-02T19:07:30.333492",
                    3,
                    "Andrew",
                    "Contacted Maintainer",
                ],
            ]
            assert TestDataFreshnessStatus.cells_result == result
            sheet.row_limit = 5
            freshness.process_broken()
            assert TestDataFreshnessStatus.cells_result == result[:6]

            now = parse_date("2017-01-31 19:07:30.333492", include_microseconds=True)
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )

            sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Broken1
            sheet.dutyofficer = {"name": "Peter", "email": "peter@lala.org"}
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_broken()
            assert TestDataFreshnessStatus.email_users_result == list()
            assert TestDataFreshnessStatus.cells_result is None

    def test_freshnessstatus(
        self, configuration, database_status, users, organizations
    ):
        site_url = "http://lala"
        sysadmin_emails = ["blah2@blah.com", "blah4@blah.com"]
        now = parse_date("2017-02-02 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=self.email_users,
        )
        with Database(**database_status, table_base=Base) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            sheet.issues_spreadsheet = (
                TestDataFreshnessStatus.TestSpreadsheet_OverdueDelinquent
            )
            sheet.dutyofficer = {"name": "Sharon", "email": "sharon@lala.org"}

            TestDataFreshnessStatus.email_users_result = list()
            freshness.check_number_datasets(now, send_failures=list())
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["sharon@lala.org"],
                    "WARNING: Fall in datasets on HDX today! (02/02/2017)",
                    "Dear Sharon,\n\nThere are 1 (17%) fewer datasets today than yesterday on HDX which may indicate a serious problem so should be investigated!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Sharon,<br><br>There are 1 (17%) fewer datasets today than yesterday on HDX which may indicate a serious problem so should be investigated!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                )
            ]

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_delinquent()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["sharon@lala.org"],
                    "Delinquent datasets (02/02/2017)",
                    "Dear Sharon,\n\nThe following datasets have just become delinquent and their maintainers should be approached:\n\nAirports in Mayotte (http://lala/dataset/ourairports-myt) from OurAirports maintained by blah5full (blah5@blah.com) with expected update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Sharon,<br><br>The following datasets have just become delinquent and their maintainers should be approached:<br><br><a href="http://lala/dataset/ourairports-myt">Airports in Mayotte</a> from OurAirports maintained by <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                )
            ]

            assert TestDataFreshnessStatus.cells_result == [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/ourairports-myt",
                    "Airports in Mayotte",
                    "OurAirports",
                    "blah5full",
                    "blah5@blah.com",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:32:32.025059",
                    "2017-01-01T19:07:30.333492",
                    "2017-02-02T19:07:30.333492",
                    3,
                    "Peter",
                    "Done",
                ],
                [
                    "http://lala/dataset/ourairports-wsm",
                    "Airports in Samoa",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:32:30.661408",
                    "2017-02-02T19:07:30.333492",
                    "2017-02-02T19:07:30.333492",
                    1,
                    "Sharon",
                    "",
                ],
            ]

            expected_result = [
                (
                    ["blah@blah.com"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: Every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: Every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["blah4@blah.com"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["blah5@blah.com"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["sharon@lala.org"],
                    "All overdue dataset emails (02/02/2017)",
                    "Dear Sharon,\n\nBelow are the emails which have been sent today to maintainers whose datasets are overdue. You may wish to follow up with them.\n\nDear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: Every six months\nDear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\nDear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Sharon,<br><br>Below are the emails which have been sent today to maintainers whose datasets are overdue. You may wish to follow up with them.<br><br>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: Every six months<br>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
            ]

            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue()
            assert TestDataFreshnessStatus.email_users_result == expected_result
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue(sysadmins=["elizabeth@lala.org"])
            restuple = expected_result[3]
            expected_result[3] = (
                ["elizabeth@lala.org"],
                restuple[1],
                restuple[2].replace("Sharon", "system administrator"),
                restuple[3].replace("Sharon", "system administrator"),
                None,
                restuple[5],
            )
            assert TestDataFreshnessStatus.email_users_result == expected_result
            TestDataFreshnessStatus.email_users_result = list()
            sheet.dutyofficer = None
            freshness.process_overdue()
            restuple = expected_result[3]
            expected_result[3] = (
                ["blah2@blah.com", "blah4@blah.com"],
                restuple[1],
                restuple[2],
                restuple[3],
                None,
                restuple[5],
            )
            assert TestDataFreshnessStatus.email_users_result == expected_result
            TestDataFreshnessStatus.email_users_result = list()
            sheet.dutyofficer = {"name": "Paul", "email": "paul@lala.org"}
            freshness.process_overdue(recipients=["lizzy@lala.org"])
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["lizzy@lala.org"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: Every six months\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: Every six months<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["lizzy@lala.org"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["lizzy@lala.org"],
                    "Time to update your datasets on HDX (02/02/2017)",
                    'Dear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nTip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.\n\nBest wishes,\nHDX Team',
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your <a href="https://data.humdata.org/dashboard/datasets">dashboard</a> on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Tip: You can decrease the "Expected Update Frequency" by clicking "Edit" on the top right of the dataset.<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                ),
                (
                    ["paul@lala.org"],
                    "All overdue dataset emails (02/02/2017)",
                    "Dear Paul,\n\nBelow are the emails which have been sent today to maintainers whose datasets are overdue. You may wish to follow up with them.\n\nDear blahdisp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) with expected update frequency: Every six months\nDear blah4disp,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\nDear blah5full,\n\nThe dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Paul,<br><br>Below are the emails which have been sent today to maintainers whose datasets are overdue. You may wish to follow up with them.<br><br>Dear blahdisp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> with expected update frequency: Every six months<br>Dear blah4disp,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br>Dear blah5full,<br><br>The dataset(s) listed below are due for an update on the Humanitarian Data Exchange (HDX). You can update all of these in your dashboard on HDX.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
            ]

            now = parse_date("2017-01-31 19:07:30.333492", include_microseconds=True)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            sheet.issues_spreadsheet = (
                TestDataFreshnessStatus.TestSpreadsheet_OverdueDelinquent
            )
            sheet.dutyofficer = {"name": "Sharon", "email": "sharon@lala.org"}

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_delinquent()
            assert TestDataFreshnessStatus.email_users_result == list()
            assert TestDataFreshnessStatus.cells_result is None
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_overdue()
            assert TestDataFreshnessStatus.email_users_result == list()

    def test_freshnessmaintainerorgadmins(
        self, configuration, database_maintainer, users, organizations
    ):
        site_url = "http://lala"
        sysadmin_emails = ["blah2@blah.com", "blah4@blah.com"]
        now = parse_date("2017-02-02 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=self.email_users,
        )
        with Database(**database_maintainer, table_base=Base) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            sheet.issues_spreadsheet = (
                TestDataFreshnessStatus.TestSpreadsheet_MaintainerOrgAdmins
            )
            sheet.dutyofficer = {"name": "Aaron", "email": "aaron@lala.org"}

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["aaron@lala.org"],
                    "Datasets with invalid maintainer (02/02/2017)",
                    "Dear Aaron,\n\nThe following datasets have an invalid maintainer and should be checked:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following datasets have an invalid maintainer and should be checked:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
                (
                    ["aaron@lala.org"],
                    "Organizations with invalid admins (02/02/2017)",
                    "Dear Aaron,\n\nThe following organizations have an invalid administrator and should be checked:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: All org admins are sysadmins!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following organizations have an invalid administrator and should be checked:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: All org admins are sysadmins!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
            ]

            assert TestDataFreshnessStatus.cells_result == [
                [
                    [
                        "URL",
                        "Title",
                        "Organisation",
                        "Maintainer",
                        "Maintainer Email",
                        "Org Admins",
                        "Org Admin Emails",
                        "Update Frequency",
                        "Latest of Modifieds",
                        "Date Added",
                        "Date Last Occurred",
                        "No. Times",
                        "Assigned",
                        "Status",
                    ],
                    [
                        "http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015",
                        "Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015",
                        "OCHA Colombia",
                        "blahdisp",
                        "blah@blah.com",
                        "blah3disp,blah5full",
                        "blah3@blah.com,blah5@blah.com",
                        "Every six months",
                        "2016-07-17T10:25:57.626518",
                        "2017-02-02T19:07:30.333492",
                        "2017-02-02T19:07:30.333492",
                        1,
                        "Aaron",
                        "",
                    ],
                    [
                        "http://lala/dataset/projected-ipc-population-estimates-february-june-2016",
                        "Projected IPC population Estimates February - June 2016",
                        "OCHA Somalia",
                        "blahdisp",
                        "blah@blah.com",
                        "blah3disp,blah4disp",
                        "blah3@blah.com,blah4@blah.com",
                        "Every six months",
                        "2016-07-17T10:13:34.099517",
                        "2017-02-02T19:07:30.333492",
                        "2017-02-02T19:07:30.333492",
                        1,
                        "Aaron",
                        "",
                    ],
                    [
                        "http://lala/dataset/yemen-admin-boundaries",
                        "Yemen - Administrative Boundaries",
                        "OCHA Yemen",
                        "",
                        "",
                        "blah4disp,blah5full",
                        "blah4@blah.com,blah5@blah.com",
                        "Every year",
                        "2015-12-28T06:39:20.134647",
                        "2017-02-02T19:07:30.333492",
                        "2017-02-02T19:07:30.333492",
                        1,
                        "Aaron",
                        "",
                    ],
                    [
                        "http://lala/dataset/ourairports-wsm",
                        "Airports in Samoa",
                        "OurAirports",
                        "",
                        "",
                        "blah3disp,blah4disp,blah5full",
                        "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                        "Every year",
                        "2015-11-24T23:32:30.661408",
                        "2017-02-02T19:07:30.333492",
                        "2017-02-02T19:07:30.333492",
                        1,
                        "Aaron",
                        "",
                    ],
                    [
                        "http://lala/dataset/ourairports-myt",
                        "Airports in Mayotte",
                        "OurAirports",
                        "blah5full",
                        "blah5@blah.com",
                        "blah3disp,blah4disp,blah5full",
                        "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                        "Every year",
                        "2015-11-24T23:32:32.025059",
                        "2017-01-01T19:07:30.333492",
                        "2017-01-01T19:07:30.333492",
                        3,
                        "Aaron",
                        "Done",
                    ],
                ],
                [
                    [
                        "URL",
                        "Title",
                        "Error",
                        "Date Added",
                        "Date Last Occurred",
                        "No. Times",
                        "Assigned",
                        "Status",
                    ],
                    [
                        "http://lala/organization/ocha-somalia",
                        "OCHA Somalia",
                        "All org admins are sysadmins!",
                        "2017-01-01T19:07:30.333492",
                        "2017-02-02T19:07:30.333492",
                        6,
                        "Aaron",
                        "",
                    ],
                ],
            ]

            neworgs = copy.deepcopy(organizations)
            neworgs[0]["users"].append(
                {
                    "capacity": "editor",
                    "id": "blah",
                    "name": "blahname",
                    "sysadmin": False,
                    "fullname": "blahfull",
                    "display_name": "blahdisp",
                }
            )
            neworgs[1]["users"] = list()
            hdxhelper = HDXHelper(site_url=site_url, users=users, organizations=neworgs)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["aaron@lala.org"],
                    "Datasets with invalid maintainer (02/02/2017)",
                    "Dear Aaron,\n\nThe following datasets have an invalid maintainer and should be checked:\n\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following datasets have an invalid maintainer and should be checked:<br><br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
                (
                    ["aaron@lala.org"],
                    "Organizations with invalid admins (02/02/2017)",
                    "Dear Aaron,\n\nThe following organizations have an invalid administrator and should be checked:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: No org admins defined!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following organizations have an invalid administrator and should be checked:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: No org admins defined!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
            ]

            neworgs = copy.deepcopy(organizations)
            neworgs[0]["users"].append(
                {
                    "capacity": "member",
                    "id": "blah",
                    "name": "blahname",
                    "sysadmin": False,
                    "fullname": "blahfull",
                    "display_name": "blahdisp",
                }
            )
            neworgs[1]["users"][0]["id"] = "NOTEXIST1"
            neworgs[1]["users"][1]["id"] = "NOTEXIST2"
            hdxhelper = HDXHelper(site_url=site_url, users=users, organizations=neworgs)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_maintainer_orgadmins()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["aaron@lala.org"],
                    "Datasets with invalid maintainer (02/02/2017)",
                    "Dear Aaron,\n\nThe following datasets have an invalid maintainer and should be checked:\n\nTendencias Humanitarias y Paz - Nov 2012 - Dic 2015 (http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015) from OCHA Colombia maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months\nProjected IPC population Estimates February - June 2016 (http://lala/dataset/projected-ipc-population-estimates-february-june-2016) from OCHA Somalia maintained by blahdisp (blah@blah.com) with expected update frequency: Every six months\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) from OCHA Yemen with missing maintainer and organization administrators blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following datasets have an invalid maintainer and should be checked:<br><br><a href="http://lala/dataset/tendencias-humanitarias-y-paz-dic-2015">Tendencias Humanitarias y Paz - Nov 2012 - Dic 2015</a> from OCHA Colombia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/projected-ipc-population-estimates-february-june-2016">Projected IPC population Estimates February - June 2016</a> from OCHA Somalia maintained by <a href="mailto:blah@blah.com">blahdisp</a> with expected update frequency: Every six months<br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> from OCHA Yemen with missing maintainer and organization administrators <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
                (
                    ["aaron@lala.org"],
                    "Organizations with invalid admins (02/02/2017)",
                    "Dear Aaron,\n\nThe following organizations have an invalid administrator and should be checked:\n\nOCHA Somalia (http://lala/organization/ocha-somalia) with error: The following org admins do not exist: NOTEXIST1, NOTEXIST2!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Aaron,<br><br>The following organizations have an invalid administrator and should be checked:<br><br><a href="http://lala/organization/ocha-somalia">OCHA Somalia</a> with error: The following org admins do not exist: NOTEXIST1, NOTEXIST2!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                ),
            ]

    def test_freshnessfailure(
        self, configuration, database_failure, users, organizations
    ):
        site_url = ""
        TestDataFreshnessStatus.email_users_result = list()
        now = parse_date("2017-02-03 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        email = Email(
            now,
            send_emails=self.email_users,
        )
        with Database(**database_failure, table_base=Base) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            freshness.check_number_datasets(
                now, send_failures=["blah2@blah.com", "blah4@blah.com"]
            )
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["blah2@blah.com", "blah4@blah.com"],
                    "FAILURE: No datasets today! (03/02/2017)",
                    "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                )
            ]
            TestDataFreshnessStatus.email_users_result = list()
            now = parse_date("2017-02-02 19:07:30.333492", include_microseconds=True)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            freshness.check_number_datasets(
                parse_date("2017-02-01 19:07:30.333492", include_microseconds=True),
                send_failures=["blah2@blah.com", "blah4@blah.com"],
            )
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["blah2@blah.com", "blah4@blah.com"],
                    "FAILURE: Future run date! (03/02/2017)",
                    "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                )
            ]
            TestDataFreshnessStatus.email_users_result = list()
            now = parse_date("2017-02-04 19:07:30.333492", include_microseconds=True)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            freshness.check_number_datasets(
                now, send_failures=["blah2@blah.com", "blah4@blah.com"]
            )
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["blah2@blah.com", "blah4@blah.com"],
                    "FAILURE: No run today! (03/02/2017)",
                    "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                )
            ]
            TestDataFreshnessStatus.email_users_result = list()
            now = parse_date("2017-02-04 19:07:30.333492", include_microseconds=True)
            # insert new run and dataset
            run_date = parse_date(
                "2017-02-04 9:07:30.333492", include_microseconds=True
            )
            dbrun = DBRun(run_number=3, run_date=run_date)
            session.add(dbrun)
            dbdataset = DBDataset(
                run_number=3,
                id="lala",
                dataset_date="",
                update_frequency=0,
                review_date=run_date,
                last_modified=run_date,
                updated_by_script=run_date,
                metadata_modified=run_date,
                latest_of_modifieds=run_date,
                what_updated="lala",
                last_resource_updated="lala",
                last_resource_modified=run_date,
                fresh=0,
                error=False,
            )
            session.add(dbdataset)
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )

            freshness.check_number_datasets(
                now, send_failures=["blah2@blah.com", "blah4@blah.com"]
            )
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["blah2@blah.com", "blah4@blah.com"],
                    "FAILURE: Previous run corrupted! (03/02/2017)",
                    "Dear system administrator,\n\nIt is highly probable that data freshness has failed!\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear system administrator,<br><br>It is highly probable that data freshness has failed!<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    None,
                    None,
                )
            ]

    def test_freshnessdatasetsnoresources(
        self, configuration, database_noresources, users, organizations
    ):
        site_url = "http://lala"
        sysadmin_emails = ["blah2@blah.com", "blah4@blah.com"]
        now = parse_date("2017-02-03 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=self.email_users,
        )
        with Database(**database_noresources, table_base=Base) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            sheet.issues_spreadsheet = (
                TestDataFreshnessStatus.TestSpreadsheet_NoResources
            )
            sheet.dutyofficer = {"name": "Andrew", "email": "andrew@lala.org"}

            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
            freshness.process_datasets_noresources()
            assert TestDataFreshnessStatus.email_users_result == [
                (
                    ["andrew@lala.org"],
                    "Datasets with no resources (03/02/2017)",
                    "Dear Andrew,\n\nThe following datasets have no resources and should be checked:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Andrew,<br><br>The following datasets have no resources and should be checked:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["blah2@blah.com", "blah4@blah.com"],
                    None,
                )
            ]

            assert TestDataFreshnessStatus.cells_result == [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/ourairports-wsm",
                    "Airports in Samoa",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:32:30.661408",
                    "2017-02-03T19:07:30.333492",
                    "2017-02-03T19:07:30.333492",
                    1,
                    "Andrew",
                    "",
                ],
                [
                    "http://lala/dataset/ourairports-myt",
                    "Airports in Mayotte",
                    "OurAirports",
                    "blah5full",
                    "blah5@blah.com",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "Every year",
                    "2015-11-24T23:32:32.025059",
                    "2017-01-01T19:07:30.333492",
                    "2017-01-01T19:07:30.333492",
                    2,
                    "Peter",
                    "Done",
                ],
            ]

    # def test_time_period(self, configuration, database_datasets_modified_yesterday, users, organizations):
    #     site_url = 'http://lala'
    #     sysadmin_emails = ['blah3@blah.com']
    #     now = parse_date('2017-02-02 19:07:30.333492', include_microseconds=True)
    #     sheet = Sheet(now)
    #     email = Email(now, send_emails=self.email_users, sysadmin_emails=sysadmin_emails)
    #     with Database(**database_datasets_modified_yesterday) as database:
    #         session = database.get_session()
    #         hdxhelper = HDXHelper(site_url=site_url, users=users, organizations=organizations)
    #         databasequeries = DatabaseQueries(session=session, now=now, hdxhelper=hdxhelper)
    #         freshness = DataFreshnessStatus(hdxhelper=hdxhelper, databasequeries=databasequeries, email=email,
    #                                         sheet=sheet)
    #         sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Empty
    #         sheet.dutyofficer = {'name': 'Sharon', 'email': 'sharon@lala.org'}
    #
    #         TestDataFreshnessStatus.email_users_result = list()
    #         TestDataFreshnessStatus.cells_result = None
    #         freshness.process_datasets_time_period()
    #         expected_result = \
    #             [(['blah4@blah.com'],
    #               'Check time period for your datasets on HDX (02/02/2017)',
    #               'Dear blah4disp,\n\nThe dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year and time period: 11/01/2015\n\nBest wishes,\nHDX Team',
    #               '<html>\n  <head></head>\n  <body>\n    <span>Dear blah4disp,<br><br>The dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year and time period: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
    #               None, None),
    #              (['blah5@blah.com'], 'Check time period for your datasets on HDX (02/02/2017)',
    #               'Dear blah5full,\n\nThe dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year and time period: 11/01/2015\n\nBest wishes,\nHDX Team',
    #               '<html>\n  <head></head>\n  <body>\n    <span>Dear blah5full,<br><br>The dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year and time period: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
    #               None, None),
    #              (['sharon@lala.org'], 'All time period emails (02/02/2017)',
    #               'Dear Sharon,\n\nBelow are the emails which have been sent today to maintainers whose datasets have a time period that has not been updated. You may wish to follow up with them.\n\nDear blah4disp,\n\nThe dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year and time period: 11/01/2015\nDear blah5full,\n\nThe dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.\n\nYemen - Administrative Boundaries (http://lala/dataset/yemen-admin-boundaries) with expected update frequency: Every year and time period: 11/01/2015\n\nBest wishes,\nHDX Team',
    #               '<html>\n  <head></head>\n  <body>\n    <span>Dear Sharon,<br><br>Below are the emails which have been sent today to maintainers whose datasets have a time period that has not been updated. You may wish to follow up with them.<br><br>Dear blah4disp,<br><br>The dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year and time period: 11/01/2015<br>Dear blah5full,<br><br>The dataset(s) listed below have a time period that has not been updated for a while. Log into the HDX platform now to check and if necessary update each dataset.<br><br><a href="http://lala/dataset/yemen-admin-boundaries">Yemen - Administrative Boundaries</a> with expected update frequency: Every year and time period: 11/01/2015<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
    #               ['blah3@blah.com'], None)]
    #
    #         expected_cells_result = \
    #             [['URL', 'Title', 'Organisation', 'Maintainer', 'Maintainer Email', 'Org Admins', 'Org Admin Emails',
    #               'Dataset Date', 'Update Frequency', 'Latest of Modifieds', 'Date Added', 'Date Last Occurred', 'No. Times', 'Assigned',
    #               'Status'],
    #              ['http://lala/dataset/yemen-admin-boundaries', 'Yemen - Administrative Boundaries', 'OCHA Yemen',
    #               '', '',
    #               'blah4disp,blah5full', 'blah4@blah.com,blah5@blah.com', '', 'Every year',
    #               '2017-02-02T10:07:30.333492', '2017-02-02T19:07:30.333492', 1, 'Sharon', '']]
    #         assert TestDataFreshnessStatus.email_users_result == expected_result
    #         assert TestDataFreshnessStatus.cells_result == expected_cells_result
    #         TestDataFreshnessStatus.email_users_result = list()
    #         TestDataFreshnessStatus.cells_result = None
    #         freshness.process_datasets_time_period(sysadmins=['mike@lala.org'])
    #         restuple = expected_result[2]
    #         expected_result[2] = (['mike@lala.org'], restuple[1], restuple[2].replace('Sharon', 'system administrator'),
    #                               restuple[3].replace('Sharon', 'system administrator'), None, restuple[5])
    #         assert TestDataFreshnessStatus.email_users_result == expected_result
    #         assert TestDataFreshnessStatus.cells_result == expected_cells_result
    #         TestDataFreshnessStatus.email_users_result = list()
    #         TestDataFreshnessStatus.cells_result = None

    def test_datasets_datagrid(
        self,
        configuration,
        database_datasets_modified_yesterday,
        users,
        organizations,
    ):
        site_url = "http://lala"
        sysadmin_emails = ["blah3@blah.com"]
        now = parse_date("2017-02-02 19:07:30.333492", include_microseconds=True)
        sheet = Sheet(now)
        error = sheet.setup_gsheet(configuration, getenv("GSHEET_AUTH"), True, False)
        assert error is None
        error = sheet.setup_input()
        assert error is None
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=self.email_users,
        )
        with Database(
            **database_datasets_modified_yesterday, table_base=Base
        ) as database:
            session = database.get_session()
            hdxhelper = HDXHelper(
                site_url=site_url, users=users, organizations=organizations
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            freshness = DataFreshnessStatus(
                databasequeries=databasequeries,
                email=email,
                sheet=sheet,
            )
            sheet.issues_spreadsheet = TestDataFreshnessStatus.TestSpreadsheet_Empty
            sheet.dutyofficer = {"name": "Sharon", "email": "sharon@lala.org"}

            TestDataFreshnessStatus.email_users_result = list()
            freshness.process_datasets_datagrid(datasetclass=self.TestDataset)
            expected_result = [
                (
                    ["nafi@abc.org"],
                    "Candidates for the datagrid (02/02/2017)",
                    "Dear Nafi,\n\nThe new datasets listed below are candidates for the data grid that you can investigate:\n\n\nDatagrid wsm:\n\nAirports in Samoa (http://lala/dataset/ourairports-wsm) from OurAirports with missing maintainer and organization administrators blah3disp (blah3@blah.com), blah4disp (blah4@blah.com), blah5full (blah5@blah.com) with expected update frequency: Every year\n\nBest wishes,\nHDX Team",
                    '<html>\n  <head></head>\n  <body>\n    <span>Dear Nafi,<br><br>The new datasets listed below are candidates for the data grid that you can investigate:<br><br><br>Datagrid wsm:<br><br><a href="http://lala/dataset/ourairports-wsm">Airports in Samoa</a> from OurAirports with missing maintainer and organization administrators <a href="mailto:blah3@blah.com">blah3disp</a>, <a href="mailto:blah4@blah.com">blah4disp</a>, <a href="mailto:blah5@blah.com">blah5full</a> with expected update frequency: Every year<br><br>Best wishes,<br>HDX Team\n      <br/><br/>\n      <small>\n        <p>\n          <a href="http://data.humdata.org ">Humanitarian Data Exchange</a>\n        </p>\n        <p>\n          <a href="http://humdata.us14.list-manage.com/subscribe?u=ea3f905d50ea939780139789d&id=d996922315 ">            Sign up for our newsletter</a> |             <a href=" https://twitter.com/humdata ">Follow us on Twitter</a>             | <a href="mailto:hdx@un.org ">Contact us</a>\n        </p>\n      </small>\n    </span>\n  </body>\n</html>\n',
                    ["godfrey@abc.org"],
                    None,
                )
            ]
            expected_cells_result = [
                [
                    "URL",
                    "Title",
                    "Organisation",
                    "Maintainer",
                    "Maintainer Email",
                    "Org Admins",
                    "Org Admin Emails",
                    "Dataset Date",
                    "Update Frequency",
                    "Latest of Modifieds",
                    "Date Added",
                    "Date Last Occurred",
                    "No. Times",
                    "Assigned",
                    "Status",
                ],
                [
                    "http://lala/dataset/ourairports-wsm",
                    "Airports in Samoa",
                    "OurAirports",
                    "",
                    "",
                    "blah3disp,blah4disp,blah5full",
                    "blah3@blah.com,blah4@blah.com,blah5@blah.com",
                    "",
                    "Every year",
                    "2017-02-02T10:07:30.333492",
                    "2017-02-02T19:07:30.333492",
                    "2017-02-02T19:07:30.333492",
                    1,
                    "Nafi",
                    "",
                ],
            ]
            assert TestDataFreshnessStatus.email_users_result == expected_result
            assert TestDataFreshnessStatus.cells_result == expected_cells_result
            TestDataFreshnessStatus.email_users_result = list()
            TestDataFreshnessStatus.cells_result = None
