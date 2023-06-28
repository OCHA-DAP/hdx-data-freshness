from os import remove
from os.path import join
from shutil import copyfile

import pytest

from hdx.database import Database
from hdx.freshness.dbactions.dbclean import DBClean
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.path import temp_dir


class TestDBClean:
    fixtures = join("tests", "fixtures", "dbactions")

    @pytest.fixture(scope="function")
    def database(self):
        dbfile = "test_freshness.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join(self.fixtures, dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    @pytest.fixture(scope="function")
    def database_brokenrun1351(self):
        dbfile = "test_freshness_brokenrun1351.db"
        dbpath = join("tests", dbfile)
        try:
            remove(dbpath)
        except FileNotFoundError:
            pass
        copyfile(join(self.fixtures, dbfile), dbpath)
        return {"dialect": "sqlite", "database": dbpath}

    def check_results(
        self,
        folder,
        cleaner,
        runs_file,
        date_str,
        exp_starting_runs,
        exp_runs=None,
        exp_success=True,
        check_enddate=True,
        fail_on_run_difference=True,
    ):
        expected_runs = join(self.fixtures, runs_file)
        actual_runs = join(folder, runs_file)
        starting_runs = cleaner.get_runs()
        assert len(starting_runs) == exp_starting_runs

        now = parse_date(date_str)
        success = cleaner.run(
            now,
            check_enddate=check_enddate,
            filepath=actual_runs,
            fail_on_run_difference=fail_on_run_difference,
        )
        assert success is exp_success
        if not success:
            return
        runs = cleaner.get_runs()
        assert len(runs) == exp_runs
        assert_files_same(actual_runs, expected_runs)

    def test_clean(self, database):
        with temp_dir(
            "test_dbclean_clean",
            delete_on_success=True,
            delete_on_failure=False,
        ) as folder:
            with Database(**database) as session:
                cleaner = DBClean(session)

                self.check_results(
                    folder, cleaner, "runs.csv", "2023-02-27", 2072, 898
                )
                self.check_results(
                    folder, cleaner, "runs2.csv", "2023-02-28", 898, 897
                )
                self.check_results(
                    folder, cleaner, "runs3.csv", "2023-03-01", 897, 897
                )
                self.check_results(
                    folder,
                    cleaner,
                    "runs3.csv",
                    "2023-03-06",
                    897,
                    exp_success=False,
                )
                self.check_results(
                    folder,
                    cleaner,
                    "runs3.csv",
                    "2023-03-06",
                    897,
                    897,
                    check_enddate=False,
                )
                self.check_results(
                    folder,
                    cleaner,
                    "runs4.csv",
                    "2023-03-07",
                    897,
                    895,
                    check_enddate=False,
                )
                self.check_results(
                    folder,
                    cleaner,
                    "runs5.csv",
                    "2023-04-01",
                    895,
                    874,
                    check_enddate=False,
                )

    def test_broken_run(self, database_brokenrun1351):
        with temp_dir(
            "test_dbclean_broken_run",
            delete_on_success=True,
            delete_on_failure=False,
        ) as folder:
            with Database(**database_brokenrun1351) as session:
                cleaner = DBClean(session)

                self.check_results(
                    folder,
                    cleaner,
                    "runs.csv",
                    "2023-02-27",
                    2072,
                    exp_success=False,
                )
                self.check_results(
                    folder,
                    cleaner,
                    "runs_broken1351.csv",
                    "2023-02-27",
                    2072,
                    898,
                    fail_on_run_difference=False,
                )
