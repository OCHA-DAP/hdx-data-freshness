"""Determine freshness for all datasets in HDX
"""
import datetime
import logging
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from dateutil.parser import ParserError, parse
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.utilities.dictandlist import (
    dict_of_lists_add,
    list_distribute_contents,
)
from sqlalchemy import and_, exists
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from ..database.dbdataset import DBDataset
from ..database.dbinfodataset import DBInfoDataset
from ..database.dborganization import DBOrganization
from ..database.dbresource import DBResource
from ..database.dbrun import DBRun
from ..testdata.serialize import (
    serialize_datasets,
    serialize_hashresults,
    serialize_now,
    serialize_results,
)
from ..utils.retrieval import Retrieval

logger = logging.getLogger(__name__)

default_no_urls_to_check = 1000


class DataFreshness:
    """Data freshness main class

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries
        testsession (Optional[sqlalchemy.orm.Session]): Session for test data or None
        datasets (Optional[List[Dataset]]): List of datasets or read from HDX if None
        now (datetime.datetime): Date to use or take current time if None
        do_touch (bool): Whether to touch HDX resources whose hash has changed
    """

    bracketed_date = re.compile(r"\((.*)\)")

    def __init__(
        self,
        session: Session,
        testsession: Optional[Session] = None,
        datasets: Optional[List[Dataset]] = None,
        now: datetime.datetime = None,
        do_touch: bool = False,
    ) -> None:
        """"""
        self.session = session
        self.urls_to_check_count = 0
        self.updated_by_script_netlocs_checked = set()
        self.never_update = 0
        self.live_update = 0
        self.asneeded_update = 0
        self.dataset_what_updated = dict()
        self.resource_what_updated = dict()
        self.resource_last_modified_count = 0
        self.resource_broken_count = 0
        self.do_touch = do_touch

        self.url_internal = "data.humdata.org"

        self.freshness_by_frequency = dict()
        for key, value in Configuration.read()["aging"].items():
            update_frequency = int(key)
            freshness_frequency = dict()
            for status in value:
                nodays = value[status]
                freshness_frequency[status] = datetime.timedelta(days=nodays)
            self.freshness_by_frequency[update_frequency] = freshness_frequency
        self.freshness_statuses = {
            0: "0: Fresh",
            1: "1: Due",
            2: "2: Overdue",
            3: "3: Delinquent",
            None: "Freshness Unavailable",
        }
        self.testsession: Optional[Session] = testsession
        if datasets is None:  # pragma: no cover
            Configuration.read().set_read_only(
                True
            )  # so that we only get public datasets
            logger.info("Retrieving all datasets from HDX")
            self.datasets: List[Dataset] = Dataset.get_all_datasets()
            Configuration.read().set_read_only(False)
            if self.testsession:
                serialize_datasets(self.testsession, self.datasets)
        else:
            self.datasets: List[Dataset] = datasets
        if now is None:  # pragma: no cover
            self.now = datetime.datetime.utcnow()
            if self.testsession:
                serialize_now(self.testsession, self.now)
        else:
            self.now = now
        self.previous_run_number = (
            self.session.query(DBRun.run_number)
            .distinct()
            .order_by(DBRun.run_number.desc())
            .first()
        )
        if self.previous_run_number is not None:
            self.previous_run_number = self.previous_run_number[0]
            self.run_number = self.previous_run_number + 1
            no_resources = self.no_resources_force_hash()
            if no_resources:
                self.no_urls_to_check = no_resources
            else:
                self.no_urls_to_check = default_no_urls_to_check
        else:
            self.previous_run_number = None
            self.run_number = 0
            self.no_urls_to_check = default_no_urls_to_check

        logger.info(f"Will force hash {self.no_urls_to_check} resources")

    def no_resources_force_hash(self) -> Optional[int]:
        """Get number of resources to force hash

        Returns:
            Optional[int]: Number of resources to force hash or None
        """

        columns = [DBResource.id, DBDataset.updated_by_script]
        filters = [
            DBResource.dataset_id == DBDataset.id,
            DBResource.run_number == self.previous_run_number,
            DBDataset.run_number == self.previous_run_number,
            DBResource.url.notlike(f"%{self.url_internal}%"),
        ]
        query = self.session.query(*columns).filter(and_(*filters))
        noscriptupdate = 0
        noresources = 0
        for result in query:
            updated_by_script = result[1]
            if updated_by_script is not None:
                noscriptupdate += 1
                continue
            noresources += 1
        if noscriptupdate == 0:
            return None
        return noresources

    def spread_datasets(self) -> None:
        """Try to arrange the list of datasets so that downloads don't keep hitting the
        same server by moving apart datasets from the same organisation

        Returns:
            None
        """
        self.datasets: List[Dataset] = list_distribute_contents(
            self.datasets, lambda x: x["organization"]["name"]
        )

    def add_new_run(self) -> None:
        """Add a new run number with corresponding date

        Returns:
            None
        """
        dbrun = DBRun(run_number=self.run_number, run_date=self.now)
        self.session.add(dbrun)
        self.session.commit()

    @staticmethod
    def prefix_what_updated(dbresource: DBResource, prefix: str) -> None:
        """Prefix the what_updated field of resource

        Args:
            dbresource (DBResource): DBResource object to change
            prefix (str): Prefix to prepend

        Returns:
            None
        """
        what_updated = f"{prefix}-{dbresource.what_updated}"
        dbresource.what_updated = what_updated

    def process_resources(
        self,
        dataset_id: str,
        previous_dbdataset: DBDataset,
        resources: List[Resource],
        updated_by_script: Optional[datetime.datetime],
        hash_ids: List[str] = None,
    ) -> Tuple[List[Tuple], Optional[str], Optional[datetime.datetime]]:
        """Process HDX dataset's resources. If the resource has not been checked for
        30 days and we are below the threshold for resource checking, then the resource
        is flagged to be hashed even if the dataset is fresh.

        Args:
            dataset_id (str): Dataset id
            previous_dbdataset (DBDataset): DBDataset object from previous run
            resources (List[Resource]): HDX resources to process
            updated_by_script (Optional[datetime.datetime]): Time script updated or None
            hash_ids (Optional[List[str]]): Resource ids to hash for testing purposes

        Returns:
            Tuple[List[Tuple], Optional[str], Optional[datetime.datetime]]:
            (resources to download, id of last resource updated, time updated)
        """
        last_resource_updated = None
        last_resource_modified = None
        dataset_resources = list()
        for resource in resources:
            resource_id = resource["id"]
            dict_of_lists_add(self.resource_what_updated, "total", resource_id)
            url = resource["url"]
            name = resource["name"]
            metadata_modified = parse(
                resource["metadata_modified"], ignoretz=True
            )
            last_modified = parse(resource["last_modified"], ignoretz=True)
            if last_resource_modified:
                if last_modified > last_resource_modified:
                    last_resource_updated = resource_id
                    last_resource_modified = last_modified
            else:
                last_resource_updated = resource_id
                last_resource_modified = last_modified
            dbresource = DBResource(
                run_number=self.run_number,
                id=resource_id,
                name=name,
                dataset_id=dataset_id,
                url=url,
                last_modified=last_modified,
                metadata_modified=metadata_modified,
                latest_of_modifieds=last_modified,
                what_updated="firstrun",
            )
            if previous_dbdataset is not None:
                try:
                    previous_dbresource = (
                        self.session.query(DBResource)
                        .filter_by(
                            id=resource_id,
                            run_number=previous_dbdataset.run_number,
                        )
                        .one()
                    )
                    if last_modified > previous_dbresource.last_modified:
                        dbresource.what_updated = "filestore"
                    else:
                        dbresource.last_modified = (
                            previous_dbresource.last_modified
                        )
                        dbresource.what_updated = "nothing"
                    if (
                        last_modified
                        <= previous_dbresource.latest_of_modifieds
                    ):
                        dbresource.latest_of_modifieds = (
                            previous_dbresource.latest_of_modifieds
                        )
                    dbresource.http_last_modified = (
                        previous_dbresource.http_last_modified
                    )
                    dbresource.md5_hash = previous_dbresource.md5_hash
                    dbresource.hash_last_modified = (
                        previous_dbresource.hash_last_modified
                    )
                    dbresource.when_checked = previous_dbresource.when_checked

                except NoResultFound:
                    pass
            self.session.add(dbresource)

            should_hash = False
            if updated_by_script:
                netloc = urlparse(url).netloc
                if netloc in self.updated_by_script_netlocs_checked:
                    dict_of_lists_add(
                        self.resource_what_updated,
                        dbresource.what_updated,
                        resource_id,
                    )
                    continue
                else:
                    should_hash = True
                    self.updated_by_script_netlocs_checked.add(netloc)
            if self.url_internal in url:
                self.prefix_what_updated(dbresource, "internal")
                dict_of_lists_add(
                    self.resource_what_updated,
                    dbresource.what_updated,
                    resource_id,
                )
                continue
            if hash_ids:
                should_hash = resource_id in hash_ids
            elif not should_hash:
                should_hash = (
                    self.urls_to_check_count < self.no_urls_to_check
                    and (
                        dbresource.when_checked is None
                        or self.now - dbresource.when_checked
                        > datetime.timedelta(days=30)
                    )
                )
            resource_format = resource["format"].lower()
            dataset_resources.append(
                (
                    url,
                    resource_id,
                    resource_format,
                    dbresource.what_updated,
                    should_hash,
                )
            )
        return dataset_resources, last_resource_updated, last_resource_modified

    def process_datasets(
        self, hash_ids: Optional[List[str]] = None
    ) -> Tuple[Dict[str, str], List[Tuple]]:
        """Process HDX datasets. Extract necessary metadata and store in the
        freshness database. Calculate an initial freshness based on the metadata
        (last modified - which can change due to filestore resource changes,
        review date - when someone clicks the reviewed button the UI,
        updated by script - scripts provide the date of update in HDX metadata)
        For datasets that are not initially fresh or which have resources that have not
        been checked in the last 30 days (up to the threshold for the number of
        resources to check), the resources are flagged to be downloaded and hashed.

        Args:
            hash_ids (Optional[List[str]]): Resource ids to hash for testing purposes

        Returns:
            Tuple[Dict[str, str], List[Tuple]]: (datasets to check, resources to check)
        """
        resources_to_check = list()
        datasets_to_check = dict()
        logger.info("Processing datasets")
        for dataset in self.datasets:
            resources = dataset.get_resources()
            if dataset.is_requestable():  # ignore requestable
                continue
            dataset_id = dataset["id"]
            dict_of_lists_add(self.dataset_what_updated, "total", dataset_id)
            organization_id = dataset["organization"]["id"]
            organization_name = dataset["organization"]["name"]
            organization_title = dataset["organization"]["title"]
            try:
                dborganization = (
                    self.session.query(DBOrganization)
                    .filter_by(id=organization_id)
                    .one()
                )
                dborganization.name = organization_name
                dborganization.title = organization_title
            except NoResultFound:
                dborganization = DBOrganization(
                    name=organization_name,
                    id=organization_id,
                    title=organization_title,
                )
                self.session.add(dborganization)
            dataset_name = dataset["name"]
            dataset_title = dataset["title"]
            dataset_private = dataset["private"]
            dataset_maintainer = dataset["maintainer"]
            dataset_location = ",".join([x["name"] for x in dataset["groups"]])
            try:
                dbinfodataset = (
                    self.session.query(DBInfoDataset)
                    .filter_by(id=dataset_id)
                    .one()
                )
                dbinfodataset.name = dataset_name
                dbinfodataset.title = dataset_title
                dbinfodataset.private = dataset_private
                dbinfodataset.organization_id = organization_id
                dbinfodataset.maintainer = dataset_maintainer
                dbinfodataset.location = dataset_location
            except NoResultFound:
                dbinfodataset = DBInfoDataset(
                    name=dataset_name,
                    id=dataset_id,
                    title=dataset_title,
                    private=dataset_private,
                    organization_id=organization_id,
                    maintainer=dataset_maintainer,
                    location=dataset_location,
                )
                self.session.add(dbinfodataset)
            try:
                previous_dbdataset = (
                    self.session.query(DBDataset)
                    .filter_by(
                        run_number=self.previous_run_number, id=dataset_id
                    )
                    .one()
                )
            except NoResultFound:
                previous_dbdataset = None

            update_frequency = dataset.get("data_update_frequency")
            updated_by_script = None
            if update_frequency is not None:
                update_frequency = int(update_frequency)
                updated_by_script = dataset.get("updated_by_script")
                if updated_by_script:
                    if "freshness_ignore" in updated_by_script:
                        updated_by_script = None
                    else:
                        match = self.bracketed_date.search(updated_by_script)
                        if match is None:
                            updated_by_script = None
                        else:
                            try:
                                updated_by_script = parse(
                                    match.group(1), ignoretz=True
                                )
                            except ParserError:
                                updated_by_script = None
            (
                dataset_resources,
                last_resource_updated,
                last_resource_modified,
            ) = self.process_resources(
                dataset_id,
                previous_dbdataset,
                resources,
                updated_by_script,
                hash_ids=hash_ids,
            )
            dataset_date = dataset.get("dataset_date")
            metadata_modified = parse(
                dataset["metadata_modified"], ignoretz=True
            )
            if "last_modified" in dataset:
                last_modified = parse(dataset["last_modified"], ignoretz=True)
            else:
                last_modified = datetime.datetime(1970, 1, 1, 0, 0)
            if len(resources) == 0 and last_resource_updated is None:
                last_resource_updated = "NO RESOURCES"
                last_resource_modified = datetime.datetime(1970, 1, 1, 0, 0)
                error = True
                what_updated = "no resources"
            else:
                error = False
                what_updated = "firstrun"
            review_date = dataset.get("review_date")
            if review_date is None:
                latest_of_modifieds = last_modified
            else:
                review_date = parse(review_date, ignoretz=True)
                if review_date > last_modified:
                    latest_of_modifieds = review_date
                else:
                    latest_of_modifieds = last_modified
            if updated_by_script and updated_by_script > latest_of_modifieds:
                latest_of_modifieds = updated_by_script

            fresh = None
            if update_frequency is not None and not error:
                if update_frequency == 0:
                    fresh = 0
                    self.live_update += 1
                elif update_frequency == -1:
                    fresh = 0
                    self.never_update += 1
                elif update_frequency == -2:
                    fresh = 0
                    self.asneeded_update += 1
                else:
                    fresh = self.calculate_freshness(
                        latest_of_modifieds, update_frequency
                    )

            dbdataset = DBDataset(
                run_number=self.run_number,
                id=dataset_id,
                dataset_date=dataset_date,
                update_frequency=update_frequency,
                review_date=review_date,
                last_modified=last_modified,
                metadata_modified=metadata_modified,
                updated_by_script=updated_by_script,
                latest_of_modifieds=latest_of_modifieds,
                what_updated=what_updated,
                last_resource_updated=last_resource_updated,
                last_resource_modified=last_resource_modified,
                fresh=fresh,
                error=error,
            )
            if previous_dbdataset is not None and not error:
                dbdataset.what_updated = self.add_what_updated(
                    dbdataset.what_updated, "nothing"
                )
                if (
                    last_modified > previous_dbdataset.last_modified
                ):  # filestore update would cause this
                    dbdataset.what_updated = self.add_what_updated(
                        dbdataset.what_updated, "filestore"
                    )
                else:
                    dbdataset.last_modified = previous_dbdataset.last_modified
                if previous_dbdataset.review_date is None:
                    if review_date is not None:
                        dbdataset.what_updated = self.add_what_updated(
                            dbdataset.what_updated, "review date"
                        )
                else:
                    if (
                        review_date is not None
                        and review_date > previous_dbdataset.review_date
                    ):  # someone clicked the review button
                        dbdataset.what_updated = self.add_what_updated(
                            dbdataset.what_updated, "review date"
                        )
                    else:
                        dbdataset.review_date = previous_dbdataset.review_date
                if updated_by_script and (
                    previous_dbdataset.updated_by_script is None
                    or updated_by_script > previous_dbdataset.updated_by_script
                ):  # new script update of datasets
                    dbdataset.what_updated = self.add_what_updated(
                        dbdataset.what_updated, "script update"
                    )
                else:
                    dbdataset.updated_by_script = (
                        previous_dbdataset.updated_by_script
                    )
                if (
                    last_resource_modified
                    <= previous_dbdataset.last_resource_modified
                ):
                    # we keep this so that although we don't normally use it,
                    # we retain the ability to run without touching CKAN
                    dbdataset.last_resource_updated = (
                        previous_dbdataset.last_resource_updated
                    )
                    dbdataset.last_resource_modified = (
                        previous_dbdataset.last_resource_modified
                    )
                if (
                    latest_of_modifieds
                    < previous_dbdataset.latest_of_modifieds
                ):
                    dbdataset.latest_of_modifieds = (
                        previous_dbdataset.latest_of_modifieds
                    )
                    if update_frequency is not None and update_frequency > 0:
                        fresh = self.calculate_freshness(
                            previous_dbdataset.latest_of_modifieds,
                            update_frequency,
                        )
                        dbdataset.fresh = fresh
            self.session.add(dbdataset)

            update_string = f"{self.freshness_statuses[fresh]}, Updated {dbdataset.what_updated}"
            anyresourcestohash = False
            for (
                url,
                resource_id,
                resource_format,
                what_updated,
                should_hash,
            ) in dataset_resources:
                if not should_hash:
                    if (
                        fresh == 0 and update_frequency != 1
                    ) or update_frequency is None:
                        dict_of_lists_add(
                            self.resource_what_updated,
                            what_updated,
                            resource_id,
                        )
                        continue
                resources_to_check.append(
                    (url, resource_id, resource_format, what_updated)
                )
                self.urls_to_check_count += 1
                anyresourcestohash = True
            if anyresourcestohash:
                datasets_to_check[dataset_id] = update_string
            else:
                dict_of_lists_add(
                    self.dataset_what_updated, update_string, dataset_id
                )
        self.session.commit()
        return datasets_to_check, resources_to_check

    def check_urls(
        self,
        resources_to_check: List[Tuple],
        user_agent: str,
        results: Optional[Dict] = None,
        hash_results: Optional[Dict] = None,
    ) -> Tuple[Dict[str, Tuple], Dict[str, Tuple]]:
        """Download resources and hash them. If the hash has changed compared to the
        previous run, download and hash again. Return two dictionaries, the first
        with the hashes from the first downloads and the second with the hashes from
        the second downloads.

        Args:
            resources_to_check (List[Tuple]): List of resources to be checked
            user_agent (str): User agent string to use when downloading
            results (Optional[Dict]): Test results to use in place of first downloads
            hash_results (Optional[Dict]): Test results replacing second downloads

        Returns:
            Tuple[Dict[str, Tuple], Dict[str, Tuple]]:
            (results of first download, results of second download)
        """

        def get_netloc(x):
            return urlparse(x[0]).netloc

        retrieval = Retrieval(user_agent, self.url_internal)
        if results is None:  # pragma: no cover
            resources_to_check = list_distribute_contents(
                resources_to_check, get_netloc
            )
            results = retrieval.retrieve(resources_to_check)
            if self.testsession:
                serialize_results(self.testsession, results)

        hash_check = list()
        for resource_id in results:
            (
                url,
                resource_format,
                err,
                http_last_modified,
                hash,
                xlsx_hash,
            ) = results[resource_id]
            if hash:
                dbresource = (
                    self.session.query(DBResource)
                    .filter_by(id=resource_id, run_number=self.run_number)
                    .one()
                )
                if dbresource.md5_hash == hash:  # File unchanged
                    continue
                if (
                    xlsx_hash and dbresource.md5_hash == xlsx_hash
                ):  # File unchanged
                    continue
                hash_check.append((url, resource_id, resource_format))

        if hash_results is None:  # pragma: no cover
            hash_check = list_distribute_contents(hash_check, get_netloc)
            hash_results = retrieval.retrieve(hash_check)
            if self.testsession:
                serialize_hashresults(self.testsession, hash_results)

        return results, hash_results

    def process_results(
        self,
        results: Dict[str, Tuple],
        hash_results: Dict[str, Tuple],
        resourcecls: Union[Resource, Any] = Resource,
    ) -> Dict[str, Dict[str, Tuple]]:
        """Process the downloaded and hashed resources. If the two hashes are the same
        but different to the previous run's, the file has been changed. If the two
        hashes are different, it is an API (eg. editable Google sheet) where the hash
        constantly changes. If the file is determined to have been changed, then the
        resource on HDX is touched to update its last_modified field. Return a
        dictionary of dictionaries from dataset id to resource ids to update information
        about resources including their latest_of_modifieds.

        Args:
            results (Dict[str, Tuple]): Test results to use in place of first downloads
            hash_results (Dict[str, Tuple]): Test results replacing second downloads
            resourcecls (Union[Resource, Any]): Class to use. Defaults to Resource.

        Returns:
            Dict[str, Dict[str, Tuple]]: Dataset id to resource id to resource info
        """

        def check_broken(error):
            if error == Retrieval.toolargeerror:
                return False
            match_error = re.search(Retrieval.clienterror_regex, error)
            if match_error:
                return True
            return False

        datasets_resourcesinfo = dict()
        for resource_id in sorted(results):
            url, _, err, http_last_modified, hash, xlsx_hash = results[
                resource_id
            ]
            dbresource = (
                self.session.query(DBResource)
                .filter_by(id=resource_id, run_number=self.run_number)
                .one()
            )
            dataset_id = dbresource.dataset_id
            resourcesinfo = datasets_resourcesinfo.get(dataset_id, dict())
            what_updated = dbresource.what_updated
            update_last_modified = False
            is_broken = False
            if http_last_modified:
                if (
                    dbresource.http_last_modified is None
                    or http_last_modified > dbresource.http_last_modified
                ):
                    dbresource.http_last_modified = http_last_modified
            if hash:
                dbresource.when_checked = self.now
                if dbresource.md5_hash == hash:  # File unchanged
                    what_updated = self.add_what_updated(
                        what_updated, "same hash"
                    )
                elif (
                    xlsx_hash and dbresource.md5_hash == xlsx_hash
                ):  # File unchanged
                    what_updated = self.add_what_updated(
                        what_updated, "same hash"
                    )
                else:  # File updated
                    hash_to_set = hash
                    (
                        hash_url,
                        _,
                        hash_err,
                        hash_http_last_modified,
                        hash_hash,
                        hash_xlsx_hash,
                    ) = hash_results[resource_id]
                    if hash_http_last_modified:
                        if (
                            dbresource.http_last_modified is None
                            or hash_http_last_modified
                            > dbresource.http_last_modified
                        ):
                            dbresource.http_last_modified = (
                                hash_http_last_modified
                            )
                    if hash_hash:
                        if hash_hash != hash:
                            if (  # Check if this is an xlsx file that has been hashed
                                hash_xlsx_hash and hash_xlsx_hash == xlsx_hash
                            ):
                                hash = xlsx_hash
                                hash_hash = hash_xlsx_hash
                                hash_to_set = hash
                        if hash_hash == hash:
                            if (
                                dbresource.md5_hash is None
                            ):  # First occurrence of resource eg. first run - don't use hash
                                # for last modified field (and hence freshness calculation)
                                dbresource.what_updated = (
                                    self.add_what_updated(
                                        what_updated, "first hash"
                                    )
                                )
                                what_updated = dbresource.what_updated
                            else:
                                # Check if hash has occurred before
                                # select distinct md5_hash from dbresources where id = '714ef7b5-a303-4e4f-be2f-03b2ce2933c7' and md5_hash='2f3cd6a6fce5ad4d7001780846ad87a7';
                                if self.session.query(
                                    exists().where(
                                        and_(
                                            DBResource.id == resource_id,
                                            DBResource.md5_hash == hash,
                                        )
                                    )
                                ).scalar():
                                    dbresource.what_updated = (
                                        self.add_what_updated(
                                            what_updated, "repeat hash"
                                        )
                                    )
                                    what_updated = dbresource.what_updated
                                else:
                                    (
                                        what_updated,
                                        _,
                                    ) = self.set_latest_of_modifieds(
                                        dbresource, self.now, "hash"
                                    )
                                    dbresource.hash_last_modified = self.now
                                    update_last_modified = True
                            dbresource.api = False
                        else:
                            hash_to_set = hash_hash
                            what_updated = self.add_what_updated(
                                what_updated, "api"
                            )
                            dbresource.api = True
                    if hash_err:
                        what_updated = self.add_what_updated(
                            what_updated, "error"
                        )
                        dbresource.error = hash_err
                        if check_broken(hash_err):
                            is_broken = True
                    dbresource.md5_hash = hash_to_set
            if err:
                dbresource.when_checked = self.now
                what_updated = self.add_what_updated(what_updated, "error")
                dbresource.error = err
                if check_broken(err):
                    is_broken = True
            resourcesinfo[resource_id] = (
                dbresource.error,
                dbresource.latest_of_modifieds,
                dbresource.what_updated,
            )
            datasets_resourcesinfo[dataset_id] = resourcesinfo
            dict_of_lists_add(
                self.resource_what_updated, what_updated, resource_id
            )
            if (
                update_last_modified and self.do_touch
            ):  # Touch resource if needed
                try:
                    logger.info(
                        f"Updating last modified for resource {resource_id}"
                    )
                    resource = resourcecls.read_from_hdx(resource_id)
                    if resource:
                        last_modified = parse(resource["last_modified"])
                        dbdataset = (
                            self.session.query(DBDataset)
                            .filter_by(
                                id=dataset_id, run_number=self.run_number
                            )
                            .one()
                        )
                        update_frequency = dbdataset.update_frequency
                        if update_frequency > 0:
                            if (
                                self.calculate_freshness(
                                    last_modified, update_frequency
                                )
                                == 0
                            ):
                                dotouch = False
                            else:
                                dotouch = True
                        else:
                            dotouch = True
                        if dotouch:
                            resource[
                                "last_modified"
                            ] = dbresource.latest_of_modifieds.isoformat()
                            resource.update_in_hdx(
                                operation="patch",
                                batch_mode="KEEP_OLD",
                                skip_validation=True,
                                ignore_check=True,
                            )
                            self.resource_last_modified_count += 1
                            logger.info(
                                f"Resource last modified count: {self.resource_last_modified_count}"
                            )
                        else:
                            logger.info(
                                f"Didn't update last modified for resource {resource_id} as it is fresh!"
                            )
                    else:
                        logger.error(
                            f"Last modified update failed for id {resource_id}! Resource does not exist."
                        )
                except HDXError:
                    logger.exception(
                        f"Last modified update failed for id {resource_id}!"
                    )
            if is_broken and self.do_touch:
                try:
                    logger.info(f"Marking resource {resource_id} as broken")
                    resource = resourcecls.read_from_hdx(resource_id)
                    if resource:
                        resource.mark_broken()
                        self.resource_broken_count += 1
                        logger.info(
                            f"Resource broken count: {self.resource_broken_count}"
                        )
                    else:
                        logger.error(
                            f"Mark broken failed for id {resource_id}! Resource does not exist."
                        )
                except HDXError:
                    logger.exception(
                        f"Mark broken failed for id {resource_id}!"
                    )
        self.session.commit()
        return datasets_resourcesinfo

    def update_dataset_latest_of_modifieds(
        self,
        datasets_to_check: Dict[str, str],
        datasets_resourcesinfo: Dict[str, Dict[str, Tuple]],
    ) -> None:
        """Given the dictionary of dictionaries from dataset id to resource ids to
        update information about resources including their latest_of_modifieds, work
        out latest_of_modifieds for datasets and calculate freshness.

        Args:
            datasets_to_check (Dict[str, str]): Datasets with resources that were hashed
            datasets_resourcesinfo (Dict[str, Dict[str, Tuple]]): Dataset id to resource
            id to resource info

        Returns:
            None
        """

        for dataset_id in datasets_resourcesinfo:
            dbdataset = (
                self.session.query(DBDataset)
                .filter_by(id=dataset_id, run_number=self.run_number)
                .one()
            )
            dataset = datasets_resourcesinfo[dataset_id]
            dataset_latest_of_modifieds = dbdataset.latest_of_modifieds
            dataset_what_updated = dbdataset.what_updated
            last_resource_modified = dbdataset.last_resource_modified
            last_resource_updated = dbdataset.last_resource_updated
            all_errors = True
            for resource_id in sorted(dataset):
                (
                    err,
                    new_last_resource_modified,
                    new_last_resource_what_updated,
                ) = dataset[resource_id]
                if not err:
                    all_errors = False
                if new_last_resource_modified:
                    if new_last_resource_modified > last_resource_modified:
                        last_resource_updated = resource_id
                        last_resource_modified = new_last_resource_modified
                    if (
                        new_last_resource_modified
                        > dataset_latest_of_modifieds
                    ):
                        dataset_latest_of_modifieds = (
                            new_last_resource_modified
                        )
                        dataset_what_updated = new_last_resource_what_updated
            dbdataset.last_resource_updated = last_resource_updated
            dbdataset.last_resource_modified = last_resource_modified
            self.set_latest_of_modifieds(
                dbdataset, dataset_latest_of_modifieds, dataset_what_updated
            )
            update_frequency = dbdataset.update_frequency
            if update_frequency is not None and update_frequency > 0:
                dbdataset.fresh = self.calculate_freshness(
                    dbdataset.latest_of_modifieds, update_frequency
                )
            dbdataset.error = all_errors
            status = f"{self.freshness_statuses[dbdataset.fresh]}, Updated {dbdataset.what_updated}"
            if all_errors:
                status = f"{status},error"
            dict_of_lists_add(self.dataset_what_updated, status, dataset_id)
        self.session.commit()
        for dataset_id in datasets_to_check:
            if dataset_id in datasets_resourcesinfo:
                continue
            dict_of_lists_add(
                self.dataset_what_updated,
                datasets_to_check[dataset_id],
                dataset_id,
            )

    def output_counts(self) -> str:
        """Create and display output string

        Returns:
            str: Output string
        """

        def add_what_updated_str(hdxobject_what_updated):
            nonlocal output_str
            output_str += (
                f'\n* total: {len(hdxobject_what_updated["total"])} *'
            )
            for countstr in sorted(hdxobject_what_updated):
                if countstr != "total":
                    output_str += f",\n{countstr}: {len(hdxobject_what_updated[countstr])}"

        output_str = "\n*** Resources ***"
        add_what_updated_str(self.resource_what_updated)
        output_str += "\n\n*** Datasets ***"
        add_what_updated_str(self.dataset_what_updated)
        output_str += (
            f"\n\n{self.live_update} datasets have update frequency of Live"
        )
        output_str += (
            f"\n{self.never_update} datasets have update frequency of Never"
        )
        output_str += f"\n{self.asneeded_update} datasets have update frequency of As Needed"

        logger.info(output_str)
        return output_str

    @staticmethod
    def set_latest_of_modifieds(
        dbobject: Union[DBDataset, DBResource],
        modified_date: datetime.datetime,
        what_updated: str,
    ) -> Tuple[str, bool]:
        """Set latest of modifieds if provided date is greater than current and add
        to the Database object's what_updated field.

        Args:
            dbobject (Union[DBDataset, DBResource]): Database object to update
            modified_date (datetime.datetime): New modified date
            what_updated (str): What updated eg. hash

        Returns:
            Tuple[str, bool]: (DB object's what_updated, whether new date > current)
        """
        if modified_date > dbobject.latest_of_modifieds:
            dbobject.latest_of_modifieds = modified_date
            dbobject.what_updated = DataFreshness.add_what_updated(
                dbobject.what_updated, what_updated
            )
            update = True
        else:
            update = False
        return dbobject.what_updated, update

    @staticmethod
    def add_what_updated(prev_what_updated: str, what_updated: str):
        """Add to what_updated string any new cause of update (such as hash). "nothing"
        is removed if anything else is added.

        Args:
            prev_what_updated (str): Previous what_updated string
            what_updated (str): Additional what_updated string

        Returns:
            str: New what_updated string
        """
        if what_updated in prev_what_updated:
            return prev_what_updated
        if prev_what_updated != "nothing" and prev_what_updated != "firstrun":
            if what_updated != "nothing":
                return f"{prev_what_updated},{what_updated}"
            return prev_what_updated
        else:
            return what_updated

    def calculate_freshness(
        self, last_modified: datetime.datetime, update_frequency: int
    ) -> int:
        """Calculate freshness based on a last modified date and the expected update
        frequency. Returns 0 for fresh, 1 for due, 2 for overdue and 3 for delinquent.

        Args:
            last_modified (datetime.datetime): Last modified date
            update_frequency (int): Expected update frequency

        Returns:
            int: 0 for fresh, 1 for due, 2 for overdue and 3 for delinquent
        """
        delta = self.now - last_modified
        if (
            delta
            >= self.freshness_by_frequency[update_frequency]["Delinquent"]
        ):
            return 3
        elif delta >= self.freshness_by_frequency[update_frequency]["Overdue"]:
            return 2
        elif delta >= self.freshness_by_frequency[update_frequency]["Due"]:
            return 1
        return 0
