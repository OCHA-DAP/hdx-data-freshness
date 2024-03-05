"""Functions that perform queries of the freshness database
"""
import logging
import re
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session, aliased

from ...database.dbdataset import DBDataset
from ...database.dbinfodataset import DBInfoDataset
from ...database.dborganization import DBOrganization
from ...database.dbresource import DBResource
from ...database.dbrun import DBRun
from ...utils.retrieval import Retrieval
from .hdxhelper import HDXHelper

logger = logging.getLogger(__name__)


class DatabaseQueries:
    """A class that offers functions that query the freshness database

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries
        now (datetime): Date to use for now
        hdxhelper (HDXHelper): HDX helper object
    """

    format_mismatch_msg = "Format Mismatch"
    other_error_msg = "Server Error (may be temporary)"

    def __init__(self, session: Session, now: datetime, hdxhelper: HDXHelper):
        self.session = session
        self.now = now
        self.hdxhelper = hdxhelper
        (
            self.run_number_to_run_date,
            self.run_numbers,
        ) = self.get_cur_prev_runs()
        if len(self.run_numbers) < 2:
            logger.warning("Less than 2 runs!")
        self.datasets_modified_yesterday = None

    def get_run_numbers(self) -> List[Tuple]:
        """Get run numbers as list of tuples of the form (run number, run date)

        Returns:
             List[Tuple]: List of (run number, run date)
        """
        return self.run_numbers

    def get_cur_prev_runs(
        self,
    ) -> Tuple[Dict[int, datetime], List[Tuple]]:
        """Get run numbers in two forms in a tuple. the first form is a dictionary
        from run number to run date. The second form is list of tuples of the form
        (run number, run date)

        Returns:
             Tuple[Dict[int, datetime], List[Tuple]]:
             (run number to run date, list of run numbers an run dates)
        """
        list_run_numbers = self.session.execute(
            select(DBRun.run_number, DBRun.run_date)
            .distinct()
            .order_by(DBRun.run_number.desc())
        ).all()
        run_number_to_run_date = dict()
        run_numbers = list()
        last_ind = len(list_run_numbers) - 1
        for i, run_number_date in enumerate(list_run_numbers):
            run_no = run_number_date[0]
            run_date = run_number_date[1]
            run_number_to_run_date[run_no] = run_date
            if not run_numbers and run_date < self.now:
                if i == last_ind:
                    run_numbers = [run_number_date]
                else:
                    run_numbers = [run_number_date, list_run_numbers[i + 1]]
        return run_number_to_run_date, run_numbers

    def get_number_datasets(self) -> Tuple[int, int]:
        """Get the number of datasets today and yesterday in a tuple

        Returns:
             Tuple[int, int]: (number of datasets today, number of datasets yesterday)
        """
        datasets_today = self.session.execute(
            select(func.count(DBDataset.id)).where(
                DBDataset.run_number == self.run_numbers[0][0]
            )
        ).scalar_one()
        datasets_previous = self.session.execute(
            select(func.count(DBDataset.id)).where(
                DBDataset.run_number == self.run_numbers[1][0]
            )
        ).scalar_one()
        return datasets_today, datasets_previous

    def get_broken(self) -> Dict[str, Dict]:
        """Get dateset information categorised by error message

        Returns:
             Dict[str, Dict]: Dataset information categorised by error message
        """
        datasets = dict()
        if len(self.run_numbers) == 0:
            return datasets
        columns = [
            DBResource.id.label("resource_id"),
            DBResource.name.label("resource_name"),
            DBResource.dataset_id.label("id"),
            DBResource.error,
            DBInfoDataset.name,
            DBInfoDataset.title,
            DBInfoDataset.maintainer,
            DBOrganization.id.label("organization_id"),
            DBOrganization.title.label("organization_title"),
            DBDataset.update_frequency,
            DBDataset.latest_of_modifieds,
            DBDataset.what_updated,
            DBDataset.fresh,
        ]
        filters = [
            DBResource.dataset_id == DBInfoDataset.id,
            DBInfoDataset.organization_id == DBOrganization.id,
            DBResource.dataset_id == DBDataset.id,
            DBDataset.run_number == self.run_numbers[0][0],
            DBResource.run_number == DBDataset.run_number,
            DBResource.error.is_not(None),
            DBResource.when_checked > self.run_numbers[1][1],
        ]
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        for norows, result in enumerate(results):
            row = dict()
            for i, column in enumerate(columns):
                row[column.key] = result[i]
            error = row["error"]
            if error == Retrieval.toolargeerror:
                continue
            if Retrieval.notmatcherror in error:
                error_msg = self.format_mismatch_msg
            else:
                match_error = re.search(Retrieval.clienterror_regex, error)
                if match_error:
                    error_msg = match_error.group(0)[1:-1]
                else:
                    # some sort of Server Error which most of the time is temporary so ignore
                    continue
            datasets_error = datasets.get(error_msg, dict())
            datasets[error_msg] = datasets_error

            org_title = row["organization_title"]
            org = datasets_error.get(org_title, dict())
            datasets_error[org_title] = org

            dataset_name = row["name"]
            dataset = org.get(dataset_name, dict())
            org[dataset_name] = dataset

            resources = dataset.get("resources", list())
            dataset["resources"] = resources

            resource = {
                "id": row["resource_id"],
                "name": row["resource_name"],
                "error": error,
            }
            resources.append(resource)
            del row["resource_id"]
            del row["resource_name"]
            del row["error"]
            dataset.update(row)

        logger.info(f"SQL query returned {norows} rows.")
        return datasets

    def get_status(self, status: int) -> List[Dict]:
        """Get datasets for a given freshness status (0=fresh, 1=due, 2=overdue,
        3=delinquent)

        Args:
            status (int): Freshness status

        Returns:
            List[Dict]: List of datasets for a given freshness status
        """
        datasets = list()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return datasets
        columns = [
            DBInfoDataset.id,
            DBInfoDataset.name,
            DBInfoDataset.title,
            DBInfoDataset.maintainer,
            DBOrganization.id.label("organization_id"),
            DBOrganization.title.label("organization_title"),
            DBDataset.update_frequency,
            DBDataset.latest_of_modifieds,
            DBDataset.what_updated,
        ]
        filters = [
            DBDataset.id == DBInfoDataset.id,
            DBInfoDataset.organization_id == DBOrganization.id,
            DBDataset.fresh == status,
            DBDataset.run_number == self.run_numbers[0][0],
        ]
        if no_runs >= 2:
            # select * from dbdatasets a, dbdatasets b where a.id = b.id and a.fresh = status and a.run_number = 1 and
            # b.fresh = status - 1 and b.run_number = 0;
            DBDataset2 = aliased(DBDataset)
            columns.append(DBDataset2.what_updated.label("prev_what_updated"))
            filters.extend(
                [
                    DBDataset.id == DBDataset2.id,
                    DBDataset2.fresh == status - 1,
                    DBDataset2.run_number == self.run_numbers[1][0],
                ]
            )
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        for norows, result in enumerate(results):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            if dataset["what_updated"] == "nothing":
                dataset["what_updated"] = dataset["prev_what_updated"]
            del dataset["prev_what_updated"]
            datasets.append(dataset)

        logger.info(f"SQL query returned {norows} rows.")
        return datasets

    def get_invalid_maintainer_orgadmins(
        self,
    ) -> Tuple[List[Dict], Dict[str, Dict]]:
        """Get datasets with invalid maintainer and organisations with invalid
        administrators

        Returns:
            Tuple[List[Dict], Dict[str, Dict]]: (Datasets with invalid maintainer,
            organisations with invalid administrators)
        """
        invalid_maintainers = list()
        invalid_orgadmins = dict()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return invalid_maintainers, invalid_orgadmins
        columns = [
            DBInfoDataset.id,
            DBInfoDataset.name,
            DBInfoDataset.title,
            DBInfoDataset.maintainer,
            DBOrganization.id.label("organization_id"),
            DBOrganization.name.label("organization_name"),
            DBOrganization.title.label("organization_title"),
            DBDataset.update_frequency,
            DBDataset.latest_of_modifieds,
            DBDataset.what_updated,
        ]
        filters = [
            DBDataset.id == DBInfoDataset.id,
            DBInfoDataset.organization_id == DBOrganization.id,
            DBDataset.run_number == self.run_numbers[0][0],
        ]
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        for norows, result in enumerate(results):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            maintainer_id = dataset["maintainer"]
            organization_id = dataset["organization_id"]
            organization_name = dataset["organization_name"]
            organization = self.hdxhelper.organizations[organization_id]
            admins = organization.get("admin")

            def get_orginfo(error):
                return {
                    "id": organization_id,
                    "name": organization_name,
                    "title": dataset["organization_title"],
                    "error": error,
                }

            if admins:
                all_sysadmins = True
                nonexistantids = list()
                for adminid in admins:
                    admin = self.hdxhelper.users.get(adminid)
                    if not admin:
                        nonexistantids.append(adminid)
                    else:
                        if admin["sysadmin"] is False:
                            all_sysadmins = False
                if nonexistantids:
                    invalid_orgadmins[organization_name] = get_orginfo(
                        f"The following org admins do not exist: {', '.join(nonexistantids)}!"
                    )
                elif all_sysadmins:
                    invalid_orgadmins[organization_name] = get_orginfo(
                        "All org admins are sysadmins!"
                    )
                if maintainer_id in admins:
                    continue
            else:
                invalid_orgadmins[organization_name] = get_orginfo(
                    "No org admins defined!"
                )
            editors = organization.get("editor", [])
            if maintainer_id in editors:
                continue
            if maintainer_id in self.hdxhelper.sysadmins:
                continue
            invalid_maintainers.append(dataset)

        logger.info(f"SQL query returned {norows} rows.")
        return invalid_maintainers, invalid_orgadmins

    def get_datasets_noresources(self) -> List[Dict]:
        """Get datasets with no resources

        Returns:
            List[Dict]: Datasets with no resources
        """
        datasets_noresources = list()
        no_runs = len(self.run_numbers)
        if no_runs == 0:
            return datasets_noresources
        columns = [
            DBInfoDataset.id,
            DBInfoDataset.name,
            DBInfoDataset.title,
            DBInfoDataset.maintainer,
            DBOrganization.id.label("organization_id"),
            DBOrganization.name.label("organization_name"),
            DBOrganization.title.label("organization_title"),
            DBDataset.update_frequency,
            DBDataset.latest_of_modifieds,
            DBDataset.what_updated,
        ]
        filters = [
            DBDataset.id == DBInfoDataset.id,
            DBInfoDataset.organization_id == DBOrganization.id,
            DBDataset.run_number == self.run_numbers[0][0],
            DBDataset.what_updated == "no resources",
        ]
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        for norows, result in enumerate(results):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            datasets_noresources.append(dataset)

        logger.info(f"SQL query returned {norows} rows.")
        return datasets_noresources

    def get_datasets_modified_yesterday(self) -> Dict[str, Dict]:
        """Get datasets modified yesterday

        Returns:
            Dict[str, Dict]: Datasets modified yesterday
        """
        if self.datasets_modified_yesterday is not None:
            return self.datasets_modified_yesterday
        datasets = OrderedDict()
        no_runs = len(self.run_numbers)
        if no_runs < 2:
            return datasets
        columns = [
            DBInfoDataset.id,
            DBInfoDataset.name,
            DBInfoDataset.title,
            DBInfoDataset.maintainer,
            DBOrganization.id.label("organization_id"),
            DBOrganization.name.label("organization_name"),
            DBOrganization.title.label("organization_title"),
            DBDataset.dataset_date,
            DBDataset.update_frequency,
            DBDataset.latest_of_modifieds,
            DBDataset.what_updated,
        ]
        filters = [
            DBDataset.id == DBInfoDataset.id,
            DBInfoDataset.organization_id == DBOrganization.id,
            DBDataset.run_number == self.run_numbers[0][0],
            DBDataset.latest_of_modifieds > self.run_numbers[1][1],
        ]
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        for norows, result in enumerate(results):
            dataset = dict()
            for i, column in enumerate(columns):
                dataset[column.key] = result[i]
            datasets[dataset["id"]] = dataset
        logger.info(f"SQL query returned {norows} rows.")
        self.datasets_modified_yesterday = datasets
        return datasets

    def get_datasets_time_period(self) -> List[Dict]:
        """Get datasets with a time period that could be due for update

        Returns:
            List[Dict]: Datasets with a time period that could be due for update
        """
        datasets = self.get_datasets_modified_yesterday()
        dataset_ids = list()
        for dataset_id, dataset in datasets.items():
            if "*" in dataset["dataset_date"]:
                continue
            if dataset["update_frequency"] <= 0:
                continue
            dataset_ids.append(dataset_id)
        columns = [DBDataset.id, DBDataset.dataset_date]
        filters = [
            DBDataset.id.in_(dataset_ids),
            DBDataset.run_number == self.run_numbers[1][0],
        ]
        results = self.session.execute(select(*columns).where(*filters))
        norows = 0
        unchanged_dsdates_datasets = list()
        for norows, result in enumerate(results):
            dataset_id = result.id
            if result.dataset_date == datasets[dataset_id]["dataset_date"]:
                unchanged_dsdates_datasets.append(dataset_id)
        logger.info(f"SQL query returned {norows} rows.")
        DBDataset2 = aliased(DBDataset)
        dsdates_not_changed_within_uf = list()
        for dataset_id in unchanged_dsdates_datasets:
            filters = [
                DBDataset.id == dataset_id,
                DBDataset2.id == DBDataset.id,
                DBDataset2.run_number == DBDataset.run_number - 1,
                DBDataset.dataset_date != DBDataset2.dataset_date,
            ]
            result = self.session.scalar(
                select(DBDataset.run_number)
                .where(*filters)
                .order_by(DBDataset.run_number.desc())
                .limit(1)
            )
            delta = self.now - self.run_number_to_run_date[result.run_number]
            if delta > timedelta(
                days=datasets[dataset_id]["update_frequency"]
            ):
                dsdates_not_changed_within_uf.append(dataset_id)
        datasets_dataset_date = list()
        for dataset_id in dsdates_not_changed_within_uf:
            columns = [DBDataset.run_number, DBDataset.update_frequency]
            filters = [
                DBDataset.id == dataset_id,
                DBDataset2.id == DBDataset.id,
                DBDataset2.run_number == DBDataset.run_number - 1,
                DBDataset.what_updated != "nothing",
            ]
            results = self.session.execute(select(*columns).where(*filters))
            prevdate = self.now
            number_of_updates = 0
            number_of_updates_within_uf = 0
            for number_of_updates, result in enumerate(results):
                run_date = self.run_number_to_run_date[result.run_number]
                delta = prevdate - run_date
                if delta < timedelta(days=result.update_frequency):
                    number_of_updates_within_uf += 1
                prevdate = run_date
            if number_of_updates_within_uf / number_of_updates < 0.8:
                continue
            datasets_dataset_date.append(datasets[dataset_id])
        return datasets_dataset_date
