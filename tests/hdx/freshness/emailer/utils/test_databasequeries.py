"""
Unit tests for database queries code.

"""
from hdx.database import Database
from hdx.utilities.dateparse import parse_date

from hdx.freshness.emailer.utils.databasequeries import DatabaseQueries
from hdx.freshness.emailer.utils.hdxhelper import HDXHelper


class TestDatabaseQueries:
    def test_get_cur_prev_runs(self, configuration, database_failure):
        now = parse_date(
            "2017-02-01 19:07:30.333492", include_microseconds=True
        )
        with Database(**database_failure) as session:
            hdxhelper = HDXHelper(
                site_url="", users=list(), organizations=list()
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == [
                (
                    0,
                    parse_date(
                        "2017-02-01 09:07:30.333492", include_microseconds=True
                    ),
                )
            ]
            now = parse_date(
                "2017-02-02 19:07:30.333492", include_microseconds=True
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == [
                (
                    1,
                    parse_date(
                        "2017-02-02 09:07:30.333492", include_microseconds=True
                    ),
                ),
                (
                    0,
                    parse_date(
                        "2017-02-01 09:07:30.333492", include_microseconds=True
                    ),
                ),
            ]
            now = parse_date(
                "2017-01-31 19:07:30.333492", include_microseconds=True
            )
            databasequeries = DatabaseQueries(
                session=session, now=now, hdxhelper=hdxhelper
            )
            assert databasequeries.run_numbers == list()
