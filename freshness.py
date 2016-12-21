#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Data freshness:
---------

Calculate freshness for all datasets in HDX.

'''
import logging
import datetime

from dateutil import parser
from hdx.data.dataset import Dataset
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from database.base import Base
from database.dbdataset import DBDataset
from database.dbresource import DBResource
from database.dbrun import DBRun
from retrieval import retrieve

logger = logging.getLogger(__name__)

class Freshness:
    def __init__(self, configuration):
        ''''''
        engine = create_engine('sqlite:///datasets.db', echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()
        
        self.datasets = Dataset.get_all_datasets(configuration, False, rows=100)
        self.total_datasets = len(self.datasets)
        self.still_fresh_count = 0
        self.never_update = 0
        self.total_resources = 0
        self.datahumdataorg_count = 0
        self.managehdxrwlabsorg_count = 0
        self.proxyhxlstandardorg_count = 0
        self.ourairportscom_count = 0
        self.revision_count = 0
        self.metadata_count = 0
        self.lastmodified_count = 0
        self.hash_updated_count = 0
        self.hash_unchanged_count = 0
        self.api_count = 0
        self.failed_count = 0

        self.aging = dict()
        for key, value in configuration['aging'].items():
            period = int(key)
            aging_period = dict()
            for status in value:
                nodays = value[status]
                aging_period[status] = datetime.timedelta(days=nodays)
            self.aging[period] = aging_period
        self.now = datetime.datetime.utcnow()
        dbrun = DBRun(run_date=self.now)
        self.session.add(dbrun)
        self.session.commit()

    def process_datasets(self):
        metadata = list()
        for dataset in self.datasets:
            dataset_id = dataset['id']
            dbdataset = self.session.query(DBDataset).filter_by(id=dataset_id).\
                order_by(DBDataset.run_date.desc()).first()
            resources = dataset.get_resources()
            self.total_resources += len(resources)
            fresh = None
            update_frequency = dataset.get('data_update_frequency')
            if update_frequency is not None:
                update_frequency = int(update_frequency)
                if update_frequency == 0:
                    fresh = 0
                    self.never_update += 1
                else:
                    if dbdataset:
                        fresh = self.calculate_aging(dbdataset.last_modified, update_frequency)
                        if fresh == 0:
                            self.still_fresh_count += 1
            dataset_resources, last_resource_updated, last_resource_modified = \
                self.process_resources(dataset_id, dbdataset, resources)
            dataset_name = dataset['name']
            dataset_date = dataset.get('dataset_date')
            metadata_modified = parser.parse(dataset['metadata_modified'], ignoretz=True)
            if update_frequency:
                fresh = self.calculate_aging(metadata_modified, update_frequency)
            new_dbdataset = DBDataset(run_date=self.now, id=dataset_id, name=dataset_name, dataset_date=dataset_date,
                                      update_frequency=update_frequency, metadata_modified=metadata_modified,
                                      last_modified=metadata_modified, last_resource_updated=last_resource_updated,
                                      last_resource_modified=last_resource_modified, fresh=fresh, error=False)
            if dbdataset is None:
                if fresh == 0:
                    self.revision_count += 1
            else:
                if last_resource_modified > dbdataset.last_resource_modified:
                    if fresh == 0 and last_resource_modified > dbdataset.last_modified:
                        self.revision_count += 1
                else:
                    new_dbdataset.last_resource_updated = dbdataset.last_resource_updated
                    new_dbdataset.last_resource_modified = dbdataset.last_resource_modified
                if metadata_modified <= dbdataset.last_modified:
                    new_dbdataset.last_modified = dbdataset.last_modified
                dbdataset.fresh = fresh

            self.session.add(new_dbdataset)
            if fresh == 0:
                self.metadata_count += 1
            else:
                metadata += dataset_resources
        self.session.commit()
        return metadata

    def process_resources(self, dataset_id, dbdataset, resources):
        last_resource_updated = None
        last_resource_modified = None
        dataset_resources = list()
        for resource in resources:
            resource_id = resource['id']
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
            new_dbresource = DBResource(run_date=self.now, id=resource_id, name=name, dataset_id=dataset_id, url=url,
                                        last_modified=revision_last_updated,
                                        revision_last_updated=revision_last_updated, updated='revision')
            if dbdataset is not None:
                dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                      run_date=dbdataset.run_date).first()
                if dbresource is not None:
                    self.set_resource_last_modified(new_dbresource, dbresource.last_modified, '')
                    new_dbresource.md5_hash = dbresource.md5_hash

            if 'data.humdata.org' in url:
                self.datahumdataorg_count += 1
                continue
            if 'manage.hdx.rwlabs.org' in url:
                self.managehdxrwlabsorg_count += 1
                continue
            if 'proxy.hxlstandard.org' in url:
                self.proxyhxlstandardorg_count += 1
                continue
            if 'ourairports.com' in url:
                self.ourairportscom_count += 1
                continue
            self.session.add(new_dbresource)
            dataset_resources.append((url, resource_id))
        return dataset_resources, last_resource_updated, last_resource_modified

    def check_urls(self, metadata):
        results = retrieve(metadata)

        hash_check = list()
        for resource_id in results:
            url, status, result = results[resource_id]
            if status == 2:
                dbresource = self.session.query(DBResource).filter_by(id=resource_id, run_date=self.now).first()
                if dbresource.md5_hash != result:  # File changed
                    hash_check.append((url, resource_id))
        hash_results = retrieve(hash_check)
        return results, hash_results

    def process_results(self, results, hash_results):
        datasets_lastmodified = dict()
        for resource_id in results:
            url, status, result = results[resource_id]
            dbresource = self.session.query(DBResource).filter_by(id=resource_id, run_date=self.now).first()
            dataset_id = dbresource.dataset_id
            datasetinfo = datasets_lastmodified.get(dataset_id, dict())
            if status == 0:
                self.failed_count += 1
                dbresource.error = result
                datasetinfo[resource_id] = None
            elif status == 1:
                self.lastmodified_count += 1
                dbresource.http_last_modified = parser.parse(result, ignoretz=True)
                self.set_resource_last_modified(dbresource, dbresource.http_last_modified, 'http header')
                datasetinfo[resource_id] = dbresource.last_modified
            elif status == 2:
                if dbresource.md5_hash == result:  # File unchanged
                    self.hash_unchanged_count += 1
                else:  # File updated
                    hash_url, hash_status, hash_result = hash_results[resource_id]
                    if hash_status == 0:
                        self.failed_count += 1
                        dbresource.error = result
                        datasetinfo[resource_id] = None
                    elif hash_status == 1:
                        self.lastmodified_count += 1
                        dbresource.http_last_modified = parser.parse(result, ignoretz=True)
                        self.set_resource_last_modified(dbresource, dbresource.http_last_modified,
                                                        'http header')
                        datasetinfo[resource_id] = dbresource.last_modified
                    elif hash_status == 2:
                        dbresource.md5_hash = hash_result
                        if hash_result == result:
                            self.hash_updated_count += 1
                            self.set_resource_last_modified(dbresource, self.now, 'hash')
                        else:
                            self.api_count += 1
                            self.set_resource_last_modified(dbresource, self.now, 'api')
                    else:
                        raise ValueError('Invalid status returned!')
                datasetinfo[resource_id] = dbresource.last_modified
            else:
                raise ValueError('Invalid status returned!')
            datasets_lastmodified[dataset_id] = datasetinfo
        self.session.commit()
        return datasets_lastmodified

    def update_dataset_last_modified(self, datasets):
        for dataset_id in datasets:
            dbdataset = self.session.query(DBDataset).filter_by(id=dataset_id, run_date=self.now).first()
            dataset = datasets[dataset_id]
            dataset_last_modified = dbdataset.last_modified
            last_resource_modified = dbdataset.last_resource_modified
            last_resource_updated = dbdataset.last_resource_updated
            all_errors = True
            for resource_id in dataset:
                new_last_resource_modified = dataset[resource_id]
                if new_last_resource_modified:
                    all_errors = False
                    if new_last_resource_modified > last_resource_modified:
                        last_resource_updated = resource_id
                        last_resource_modified = new_last_resource_modified
                    if new_last_resource_modified > dataset_last_modified:
                        dataset_last_modified = new_last_resource_modified
            dbdataset.last_resource_updated = last_resource_updated
            dbdataset.last_resource_modified = last_resource_modified
            if dataset_last_modified > dbdataset.last_modified:
                dbdataset.last_modified = dataset_last_modified
                update_frequency = dbdataset.update_frequency
                if update_frequency is not None:
                    dbdataset.fresh = self.calculate_aging(dbdataset.last_modified, update_frequency)
            dbdataset.error = all_errors
        self.session.commit()

    def output_counts(self):
        str = '\nResources - Total: %d\ndata.humdata.org: %d, ' % (self.total_resources, self.datahumdataorg_count)
        str += 'manage.hdx.rwlabs.org: % d, ' % self.managehdxrwlabsorg_count
        str += 'proxy.hxlstandard.org: %d, ourairports.com: %d\n' % (self.proxyhxlstandardorg_count,
                                                                     self.ourairportscom_count)
        str += 'Last-Modified: %d, ' % self.lastmodified_count
        str += 'Hash updated: %d, Hash Unchanged: %d, API: %d\n' % (self.hash_updated_count, self.hash_unchanged_count,
                                                                    self.api_count)
        str += 'Number Failed: %d\n\n' % self.failed_count
        str += 'Datasets - Total: %s\nStill Fresh: %d, ' % (self.total_datasets, self.still_fresh_count)
        str += 'Never update frequency: %d, Dataset Metadata Updated: %d' % (self.never_update, self.metadata_count)
        if self.revision_count != 0:
            str += ', Resource Metadata Updated (subset of Dataset Metadata Updated): %d' % self.revision_count
        logger.info(str)

    @staticmethod
    def set_resource_last_modified(dbresource, modified_date, updated):
        if modified_date > dbresource.last_modified:
            dbresource.last_modified = modified_date
            dbresource.updated = updated

    def calculate_aging(self, last_modified, update_frequency):
        delta = self.now - last_modified
        if delta >= self.aging[update_frequency]['Delinquent']:
            return 3
        elif delta >= self.aging[update_frequency]['Overdue']:
            return 2
        elif delta >= self.aging[update_frequency]['Due']:
            return 1
        return 0


