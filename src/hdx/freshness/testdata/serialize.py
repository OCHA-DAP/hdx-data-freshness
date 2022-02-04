"""Functions to serialise and deserialise test data, for example for datasets
"""
import datetime
from typing import Dict, Iterable, List, Tuple

from hdx.data.dataset import Dataset
from sqlalchemy.orm import Session

from .dbtestdataset import DBTestDataset
from .dbtestdate import DBTestDate
from .dbtesthashresult import DBTestHashResult
from .dbtestresource import DBTestResource
from .dbtestresult import DBTestResult


def serialize_datasets(session: Session, datasets: List[Dataset]) -> None:
    """Serialise HDX datasets to database objects

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data
        datasets (List[Dataset]): HDX datasets

    Returns:
        None
    """
    for dataset in datasets:
        dataset_id = dataset["id"]
        dbtestdataset = DBTestDataset(
            id=dataset_id,
            organization_id=dataset["organization"]["id"],
            organization_name=dataset["organization"]["name"],
            organization_title=dataset["organization"]["title"],
            dataset_name=dataset["name"],
            dataset_title=dataset["title"],
            dataset_private=dataset["private"],
            dataset_maintainer=dataset["maintainer"],
            dataset_date=dataset.get("dataset_date"),
            update_frequency=dataset.get("data_update_frequency"),
            review_date=dataset["review_date"],
            last_modified=dataset["last_modified"],
            updated_by_script=dataset.get("updated_by_script"),
            metadata_modified=dataset["metadata_modified"],
            is_requestdata_type=dataset.get("is_requestdata_type"),
            dataset_location=",".join([x["name"] for x in dataset["groups"]]),
        )
        session.add(dbtestdataset)
        for resource in dataset.get_resources():
            dbtestresource = DBTestResource(
                id=resource["id"],
                name=resource["name"],
                dataset_id=dataset_id,
                format=resource["format"],
                url=resource["url"],
                metadata_modified=resource["metadata_modified"],
                last_modified=resource["last_modified"],
            )
            session.add(dbtestresource)
    session.commit()


def serialize_now(session: Session, now: datetime.datetime) -> None:
    """Serialise date to database object

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data
        now (datetime.datetime): Current date

    Returns:
        None
    """
    dbtestdate = DBTestDate(test_date=now)
    session.add(dbtestdate)
    session.commit()


def serialize_results(session: Session, results: Dict[str, Tuple]) -> None:
    """Serialise results of downloading and hashing urls (first time) to database
    objects

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data
        results (Dict[str, Tuple]): Results of downloading and hashing urls (1st time)

    Returns:
        None
    """
    for id in results:
        (
            url,
            resource_format,
            err,
            http_last_modified,
            hash,
            xlsx_hash,
            force_hash,
        ) = results[id]
        dbtestresult = DBTestResult(
            id=id,
            url=url,
            format=resource_format,
            err=err,
            http_last_modified=http_last_modified,
            hash=hash,
            xlsx_hash=xlsx_hash,
            force_hash=force_hash,
        )
        session.add(dbtestresult)
    session.commit()


def serialize_hashresults(
    session: Session, hash_results: Dict[str, Tuple]
) -> None:
    """Serialise results of downloading and hashing urls (second time) to database
    objects

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data
        hash_results (Dict[str, Tuple]): Results of downloading+hashing urls (2nd time)

    Returns:
        None
    """
    for id in hash_results:
        (
            url,
            resource_format,
            err,
            http_last_modified,
            hash,
            xlsx_hash,
            force_hash,
        ) = hash_results[id]
        dbtesthashresult = DBTestHashResult(
            id=id,
            url=url,
            format=resource_format,
            err=err,
            http_last_modified=http_last_modified,
            hash=hash,
            xlsx_hash=xlsx_hash,
            force_hash=force_hash,
        )
        session.add(dbtesthashresult)
    session.commit()


def deserialize_datasets(session: Session) -> Iterable[Dataset]:
    """Deserialise database objects to HDX datasets

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data

    Returns:
        Iterable[Dataset]: HDX Dataset objects
    """
    datasets = dict()
    for dbtestdataset in session.query(DBTestDataset):
        dataset_id = dbtestdataset.id
        organization = {
            "id": dbtestdataset.organization_id,
            "name": dbtestdataset.organization_name,
            "title": dbtestdataset.organization_title,
        }
        dataset = Dataset(
            {
                "id": dataset_id,
                "organization": organization,
                "name": dbtestdataset.dataset_name,
                "title": dbtestdataset.dataset_title,
                "private": dbtestdataset.dataset_private,
                "maintainer": dbtestdataset.dataset_maintainer,
                "dataset_date": dbtestdataset.dataset_date,
                "data_update_frequency": dbtestdataset.update_frequency,
                "review_date": dbtestdataset.review_date,
                "last_modified": dbtestdataset.last_modified,
                "updated_by_script": dbtestdataset.updated_by_script,
                "metadata_modified": dbtestdataset.metadata_modified,
                "groups": [
                    {"name": x}
                    for x in dbtestdataset.dataset_location.split(",")
                ],
            }
        )
        dataset.set_requestable(dbtestdataset.is_requestdata_type)
        datasets[dataset_id] = dataset
    for dbtestresource in session.query(DBTestResource):
        dataset = datasets[dbtestresource.dataset_id]
        resource = {
            "id": dbtestresource.id,
            "name": dbtestresource.name,
            "format": dbtestresource.format,
            "url": dbtestresource.url,
            "metadata_modified": dbtestresource.metadata_modified,
            "last_modified": dbtestresource.last_modified,
        }
        dataset.get_resources().append(resource)
    return datasets.values()


def deserialize_now(session: Session) -> datetime.datetime:
    """Deserialise database object to date

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data

    Returns:
        datetime.datetime: date
    """
    return session.query(DBTestDate.test_date).scalar()


def deserialize_results(session: Session) -> List[Tuple]:
    """Deserialise database objects to results of downloading and hashing urls
    (first time)

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data

    Returns:
        List[Tuple]: Results of downloading and hashing urls (first time)
    """
    results = dict()
    for dbtestresult in session.query(DBTestResult):
        results[dbtestresult.id] = (
            dbtestresult.url,
            dbtestresult.format,
            dbtestresult.err,
            dbtestresult.http_last_modified,
            dbtestresult.hash,
            dbtestresult.xlsx_hash,
        )
    return results


def deserialize_hashresults(session: Session) -> List[Tuple]:
    """Deserialise database objects to results of downloading and hashing urls
    (second time)

    Args:
        session (sqlalchemy.orm.Session): Session to use for queries for test data

    Returns:
        List[Tuple]: Results of downloading and hashing urls (second time)
    """
    hash_results = dict()
    for dbtesthashresult in session.query(DBTestHashResult):
        hash_results[dbtesthashresult.id] = (
            dbtesthashresult.url,
            dbtesthashresult.format,
            dbtesthashresult.err,
            dbtesthashresult.http_last_modified,
            dbtesthashresult.hash,
            dbtesthashresult.xlsx_hash,
        )
    return hash_results
