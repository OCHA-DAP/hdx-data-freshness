#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Data freshness:
---------

Calculate freshness for all datasets in HDX.

'''
import logging
import datetime

import pickle
from dateutil import parser
from hdx.configuration import Configuration
from hdx.data.dataset import Dataset
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm.exc import NoResultFound

from database.base import Base
from database.dbdataset import DBDataset
from database.dbresource import DBResource
from database.dbrun import DBRun
from retrieval import retrieve

logger = logging.getLogger(__name__)

class Freshness:
    def __init__(self, dbpath='datasets.db', save=False, datasets=None, now=None):
        ''''''
        engine = create_engine('sqlite:///%s' % dbpath, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()

        self.never_update = 0
        self.dataset_what_updated = dict()
        self.resource_what_updated = dict()

        self.urls_to_ignore = ['data.humdata.org', 'manage.hdx.rwlabs.org', 'proxy.hxlstandard.org', 'ourairports.com']

        self.aging = dict()
        for key, value in Configuration.read()['aging'].items():
            period = int(key)
            aging_period = dict()
            for status in value:
                nodays = value[status]
                aging_period[status] = datetime.timedelta(days=nodays)
            self.aging[period] = aging_period
        self.aging_statuses = {0: '0: Fresh', 1: '1: Due', 2: '2: Overdue', 3: '3: Delinquent',
                               None: 'Freshness Unavailable'}
        self.previous_run_number = self.session.query(DBRun.run_number).distinct().order_by(DBRun.run_number.desc()).first()
        if self.previous_run_number is not None:
            self.previous_run_number = self.previous_run_number[0]
            self.run_number = self.previous_run_number + 1
        else:
            self.previous_run_number = None
            self.run_number = 0
        self.save = save
        if datasets is None:  # pragma: no cover
            self.datasets = Dataset.get_all_datasets(include_gallery=False)
            if save:
                with open('datasets.pickle', 'wb') as fp:
                    pickle.dump(self.datasets, fp)
        else:
            self.datasets = datasets
        if now is None:  # pragma: no cover
            self.now = datetime.datetime.utcnow()
            if save:
                with open('now.pickle', 'wb') as fp:
                    pickle.dump(self.now, fp)
        else:
            self.now = now
        dbrun = DBRun(run_number=self.run_number, run_date=self.now)
        self.session.add(dbrun)
        self.session.commit()

    def process_datasets(self):
        resources_to_check = list()
        datasets_to_check = dict()
        for dataset in self.datasets:
            dataset_id = dataset['id']
            self.update_count(self.dataset_what_updated, 'total', dataset_id)
            try:
                previous_dbdataset = self.session.query(DBDataset).filter_by(run_number=self.previous_run_number,
                                                                             id=dataset_id).one()
            except NoResultFound:
                previous_dbdataset = None
            resources = dataset.get_resources()
            fresh = None
            dataset_resources, last_resource_updated, last_resource_modified = \
                self.process_resources(dataset_id, previous_dbdataset, resources)
            dataset_name = dataset['name']
            dataset_date = dataset.get('dataset_date')
            metadata_modified = parser.parse(dataset['metadata_modified'], ignoretz=True)
            update_frequency = dataset.get('data_update_frequency')
            if update_frequency is not None:
                update_frequency = int(update_frequency)
                if update_frequency == 0:
                    fresh = 0
                    self.never_update += 1
                else:
                    fresh = self.calculate_aging(metadata_modified, update_frequency)
            dbdataset = DBDataset(run_number=self.run_number, id=dataset_id, name=dataset_name, dataset_date=dataset_date,
                                      update_frequency=update_frequency, metadata_modified=metadata_modified,
                                      last_modified=metadata_modified, what_updated='metadata',
                                      last_resource_updated=last_resource_updated,
                                      last_resource_modified=last_resource_modified, fresh=fresh, error=False)
            if previous_dbdataset is not None:
                if last_resource_modified <= previous_dbdataset.last_resource_modified:
                    dbdataset.last_resource_updated = previous_dbdataset.last_resource_updated
                    dbdataset.last_resource_modified = previous_dbdataset.last_resource_modified
                if metadata_modified <= previous_dbdataset.last_modified:
                    dbdataset.last_modified = previous_dbdataset.last_modified
                    dbdataset.what_updated = 'nothing'
                    if update_frequency:
                        fresh = self.calculate_aging(previous_dbdataset.last_modified, update_frequency)
                        dbdataset.fresh = fresh

            self.session.add(dbdataset)
            update_string = '%s, Updated %s' % (self.aging_statuses[fresh], dbdataset.what_updated)
            if fresh == 0 or update_frequency is None:
                self.update_count(self.dataset_what_updated, update_string, dataset_id)
                for url, resource_id, what_updated in dataset_resources:
                    self.update_count(self.resource_what_updated, what_updated, resource_id)
            else:
                datasets_to_check[dataset_id] = update_string
                resources_to_check += dataset_resources
        self.session.commit()
        return datasets_to_check, resources_to_check

    def process_resources(self, dataset_id, previous_dbdataset, resources):
        last_resource_updated = None
        last_resource_modified = None
        dataset_resources = list()
        for resource in resources:
            resource_id = resource['id']
            self.update_count(self.resource_what_updated, 'total', resource_id)
            url = resource['url']
            name = resource['name']
            revision_last_updated = parser.parse(resource['revision_last_updated'], ignoretz=True)
            if last_resource_modified:
                if revision_last_updated > last_resource_modified:
                    last_resource_updated = resource_id
                    last_resource_modified = revision_last_updated
            else:
                last_resource_updated = resource_id
                last_resource_modified = revision_last_updated
            dbresource = DBResource(run_number=self.run_number, id=resource_id, name=name,
                                        dataset_id=dataset_id, url=url, last_modified=revision_last_updated,
                                        revision_last_updated=revision_last_updated, what_updated='revision')
            if previous_dbdataset is not None:
                try:
                    previous_dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                                   run_number=
                                                                                   previous_dbdataset.run_number).one()
                    if dbresource.last_modified <= previous_dbresource.last_modified:
                        dbresource.last_modified = previous_dbresource.last_modified
                        dbresource.what_updated = 'nothing'
                    dbresource.md5_hash = previous_dbresource.md5_hash
                except NoResultFound:
                    pass

            ignore = False
            for url_substr in self.urls_to_ignore:
                if url_substr in url:
                    self.update_count(self.resource_what_updated, url_substr, resource_id)
                    ignore = True
                    break
            if not ignore:
                self.session.add(dbresource)
                dataset_resources.append((url, resource_id, dbresource.what_updated))
        return dataset_resources, last_resource_updated, last_resource_modified

    def check_urls(self, resources_to_check, results=None, hash_results=None):
        if results is None:  # pragma: no cover
            results = retrieve(resources_to_check)
            if self.save:
                with open('results.pickle', 'wb') as fp:
                    pickle.dump(results, fp)

        hash_check = list()
        for resource_id in results:
            url, status, result = results[resource_id]
            if status == 2:
                dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                      run_number=self.run_number).one()
                if dbresource.md5_hash != result:  # File changed
                    hash_check.append((url, resource_id))

        if hash_results is None:  # pragma: no cover
            hash_results = retrieve(hash_check)
            if self.save:
                with open('hash_results.pickle', 'wb') as fp:
                    pickle.dump(hash_results, fp)
        return results, hash_results

    def process_results(self, results, hash_results):
        datasets_lastmodified = dict()
        for resource_id in sorted(results):
            url, status, result = results[resource_id]
            dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                  run_number=self.run_number).one()
            dataset_id = dbresource.dataset_id
            datasetinfo = datasets_lastmodified.get(dataset_id, dict())
            what_updated = dbresource.what_updated
            if status == 0:
                what_updated = self.add_what_updated(what_updated, 'error')
                dbresource.error = result
                datasetinfo[resource_id] = (None, None)
            elif status == 1:
                dbresource.http_last_modified = parser.parse(result, ignoretz=True)
                what_updated = self.set_last_modified(dbresource, dbresource.http_last_modified, 'http header')
                datasetinfo[resource_id] = (dbresource.last_modified, dbresource.what_updated)
            elif status == 2:
                if dbresource.md5_hash == result:  # File unchanged
                    what_updated = self.add_what_updated(what_updated, 'same hash')
                else:  # File updated
                    hash_url, hash_status, hash_result = hash_results[resource_id]
                    if hash_status == 0:
                        what_updated = self.add_what_updated(what_updated, 'error')
                        dbresource.error = result
                        datasetinfo[resource_id] = (None, None)
                    elif hash_status == 1:
                        dbresource.http_last_modified = parser.parse(result, ignoretz=True)
                        what_updated = self.set_last_modified(dbresource, dbresource.http_last_modified,
                                                        'http header')
                        datasetinfo[resource_id] = (dbresource.last_modified, dbresource.what_updated)
                    elif hash_status == 2:
                        dbresource.md5_hash = hash_result
                        if hash_result == result:
                            what_updated = self.set_last_modified(dbresource, self.now, 'hash')
                        else:
                            what_updated = self.set_last_modified(dbresource, self.now, 'api')
                        datasetinfo[resource_id] = (dbresource.last_modified, dbresource.what_updated)
                    else:
                        raise ValueError('Invalid status returned!')
            else:
                raise ValueError('Invalid status returned!')
            datasets_lastmodified[dataset_id] = datasetinfo
            self.update_count(self.resource_what_updated, what_updated, resource_id)
        self.session.commit()
        return datasets_lastmodified

    def update_dataset_last_modified(self, datasets_to_check, datasets_lastmodified):
        for dataset_id in datasets_lastmodified:
            dbdataset = self.session.query(DBDataset).filter_by(id=dataset_id,
                                                                run_number=self.run_number).one()
            dataset = datasets_lastmodified[dataset_id]
            dataset_last_modified = dbdataset.last_modified
            dataset_what_updated = dbdataset.what_updated
            last_resource_modified = dbdataset.last_resource_modified
            last_resource_updated = dbdataset.last_resource_updated
            all_errors = True
            for resource_id in sorted(dataset):
                new_last_resource_modified, new_last_resource_what_updated = dataset[resource_id]
                if new_last_resource_modified:
                    all_errors = False
                    if new_last_resource_modified > last_resource_modified:
                        last_resource_updated = resource_id
                        last_resource_modified = new_last_resource_modified
                    if new_last_resource_modified > dataset_last_modified:
                        dataset_last_modified = new_last_resource_modified
                        dataset_what_updated = new_last_resource_what_updated
            dbdataset.last_resource_updated = last_resource_updated
            dbdataset.last_resource_modified = last_resource_modified
            self.set_last_modified(dbdataset, dataset_last_modified, dataset_what_updated)
            update_frequency = dbdataset.update_frequency
            if update_frequency:
                dbdataset.fresh = self.calculate_aging(dbdataset.last_modified, update_frequency)
            dbdataset.error = all_errors
            self.update_count(self.dataset_what_updated, '%s, Updated %s' % (self.aging_statuses[dbdataset.fresh],
                                                                             dbdataset.what_updated), dataset_id)
        self.session.commit()
        for dataset_id in datasets_to_check:
            if dataset_id in datasets_lastmodified:
                continue
            self.update_count(self.dataset_what_updated, datasets_to_check[dataset_id], dataset_id)


    def output_counts(self):
        def add_what_updated_str(hdxobject_what_updated):
            nonlocal output_str
            output_str += '\n* total: %d *' % len(hdxobject_what_updated['total'])
            for countstr in sorted(hdxobject_what_updated):
                if countstr != 'total':
                    output_str += ',\n%s: %d' % (countstr, len(hdxobject_what_updated[countstr]))

        output_str = '\n*** Resources ***'
        add_what_updated_str(self.resource_what_updated)
        output_str += '\n\n*** Datasets ***'
        add_what_updated_str(self.dataset_what_updated)
        output_str += '\n\n%d datasets have update frequency of Never' % self.never_update

        logger.info(output_str)
        return output_str

    @staticmethod
    def set_last_modified(dbobject, modified_date, what_updated):
        if modified_date > dbobject.last_modified:
            dbobject.last_modified = modified_date
            dbobject.what_updated = Freshness.add_what_updated(dbobject.what_updated, what_updated)
        return dbobject.what_updated

    @staticmethod
    def add_what_updated(prev_what_updated, what_updated):
        if prev_what_updated != 'nothing':
            return '%s,%s' % (prev_what_updated, what_updated)
        else:
            return what_updated

    @staticmethod
    def update_count(hdxobject_what_updated, what_updated, hdxobject_id):
        idlist = hdxobject_what_updated.get(what_updated, list())
        idlist.append(hdxobject_id)
        hdxobject_what_updated[what_updated] = idlist

    def calculate_aging(self, last_modified, update_frequency):
        delta = self.now - last_modified
        if delta >= self.aging[update_frequency]['Delinquent']:
            return 3
        elif delta >= self.aging[update_frequency]['Overdue']:
            return 2
        elif delta >= self.aging[update_frequency]['Due']:
            return 1
        return 0


