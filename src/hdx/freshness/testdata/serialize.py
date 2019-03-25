# -*- coding: utf-8 -*-
'''
Serialize:
---------

Serialize datasets and other types

'''
from collections import OrderedDict

from hdx.data.dataset import Dataset

from hdx.freshness.testdata.dbtestdataset import DBTestDataset
from hdx.freshness.testdata.dbtestdate import DBTestDate
from hdx.freshness.testdata.dbtesthashresult import DBTestHashResult
from hdx.freshness.testdata.dbtestresource import DBTestResource
from hdx.freshness.testdata.dbtestresult import DBTestResult


def serialize_datasets(session, datasets):
    for dataset in datasets:
        dataset_id = dataset['id']
        dbtestdataset = DBTestDataset(id=dataset_id, organization_id=dataset['organization']['id'],
                                      organization_name=dataset['organization']['name'],
                                      organization_title=dataset['organization']['title'], dataset_name=dataset['name'],
                                      dataset_title=dataset['title'], dataset_private=dataset['private'],
                                      dataset_maintainer=dataset['maintainer'],
                                      dataset_maintainer_email=dataset['maintainer_email'],
                                      dataset_author=dataset['author'],
                                      dataset_author_email=dataset['author_email'],
                                      dataset_date=dataset.get('dataset_date'),
                                      metadata_modified=dataset['metadata_modified'],
                                      review_date=dataset['review_date'],
                                      last_modified=dataset['last_modified'],
                                      update_frequency=dataset.get('data_update_frequency'),
                                      is_requestdata_type=dataset.get('is_requestdata_type'),
                                      dataset_location=','.join([x['name'] for x in dataset['groups']]))
        session.add(dbtestdataset)
        for resource in dataset.get_resources():
            dbtestresource = DBTestResource(id=resource['id'], name=resource['name'], dataset_id=dataset_id,
                                            format=resource['format'], url=resource['url'],
                                            revision_last_updated=resource['revision_last_updated'],
                                            last_modified=resource['last_modified'])
            session.add(dbtestresource)
    session.commit()


def serialize_now(session, now):
    dbtestdate = DBTestDate(test_date=now)
    session.add(dbtestdate)
    session.commit()


def serialize_results(session, results):
    for id in results:
        url, err, http_last_modified, hash, force_hash = results[id]
        dbtestresult = DBTestResult(id=id, url=url, err=err, http_last_modified=http_last_modified,
                                    hash=hash, force_hash=force_hash)
        session.add(dbtestresult)
    session.commit()


def serialize_hashresults(session, hash_results):
    for id in hash_results:
        url, err, http_last_modified, hash, force_hash = hash_results[id]
        dbtesthashresult = DBTestHashResult(id=id, url=url, err=err, http_last_modified=http_last_modified,
                                            hash=hash, force_hash=force_hash)
        session.add(dbtesthashresult)
    session.commit()


def deserialize_datasets(session):
    datasets = OrderedDict()
    for dbtestdataset in session.query(DBTestDataset):
        dataset_id = dbtestdataset.id
        organization = {
            'id': dbtestdataset.organization_id,
            'name': dbtestdataset.organization_name,
            'title': dbtestdataset.organization_title
        }
        dataset = Dataset({
            'id': dataset_id,
            'organization': organization,
            'name': dbtestdataset.dataset_name,
            'title': dbtestdataset.dataset_title,
            'private': dbtestdataset.dataset_private,
            'maintainer': dbtestdataset.dataset_maintainer,
            'maintainer_email': dbtestdataset.dataset_maintainer_email,
            'author': dbtestdataset.dataset_author,
            'author_email': dbtestdataset.dataset_author_email,
            'dataset_date': dbtestdataset.dataset_date,
            'metadata_modified': dbtestdataset.metadata_modified,
            'review_date': dbtestdataset.review_date,
            'last_modified': dbtestdataset.last_modified,
            'data_update_frequency': dbtestdataset.update_frequency,
            'groups': [{'name': x} for x in dbtestdataset.dataset_location.split(',')]
        })
        dataset.set_requestable(dbtestdataset.is_requestdata_type)
        datasets[dataset_id] = dataset
    for dbtestresource in session.query(DBTestResource):
        dataset = datasets[dbtestresource.dataset_id]
        resource = {
            'id': dbtestresource.id,
            'name': dbtestresource.name,
            'format': dbtestresource.format,
            'url': dbtestresource.url,
            'revision_last_updated': dbtestresource.revision_last_updated,
            'last_modified': dbtestresource.last_modified
        }
        dataset.get_resources().append(resource)
    return datasets.values()


def deserialize_now(session):
    return session.query(DBTestDate.test_date).scalar()


def deserialize_results(session):
    results = dict()
    for dbtestresult in session.query(DBTestResult):
        results[dbtestresult.id] = (dbtestresult.url, dbtestresult.err, dbtestresult.http_last_modified,
                                    dbtestresult.hash, dbtestresult.force_hash)
    return results


def deserialize_hashresults(session):
    hash_results = dict()
    for dbtesthashresult in session.query(DBTestHashResult):
        hash_results[dbtesthashresult.id] = (dbtesthashresult.url, dbtesthashresult.err,
                                             dbtesthashresult.http_last_modified, dbtesthashresult.hash,
                                             dbtesthashresult.force_hash)
    return hash_results
