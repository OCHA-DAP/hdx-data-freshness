# -*- coding: utf-8 -*-
'''
Data freshness:
--------------

Calculate freshness for all datasets in HDX.

'''
import datetime
import logging
from urllib.parse import urlparse

from dateutil import parser
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import dict_of_lists_add, list_distribute_contents
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.pool import NullPool

from hdx.freshness.database.base import Base
from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.retrieval import retrieve
from hdx.freshness.testdata.serialize import serialize_datasets, serialize_now, serialize_results, serialize_hashresults

logger = logging.getLogger(__name__)


class DataFreshness:
    def __init__(self, db_url=None, testsession=None, datasets=None, now=None):
        ''''''
        if db_url is None:
            db_url = 'sqlite:///freshness.db'
        engine = create_engine(db_url, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        self.session = Session()

        self.urls_to_check_count = 0
        self.never_update = 0
        self.live_update = 0
        self.adhoc_update = 0
        self.dataset_what_updated = dict()
        self.resource_what_updated = dict()
        self.touch_count = 0

        self.urls_internal = ['data.humdata.org', 'manage.hdx.rwlabs.org']
        self.urls_adhoc_update = ['proxy.hxlstandard.org']

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
            no_resources = self.session.query(DBResource).filter_by(run_number=self.previous_run_number).count()
            self.no_urls_to_check = int((no_resources / 30) + 1)
        else:
            self.previous_run_number = None
            self.run_number = 0
            self.no_urls_to_check = 400

        self.testsession = testsession
        if datasets is None:  # pragma: no cover
            Configuration.read().set_read_only(True)  # so that we only get public datasets
            self.datasets = Dataset.get_all_datasets()
            Configuration.read().set_read_only(False)
            # Uncomment below 4 lines for testing
            # Configuration.delete()
            # site_url = Configuration.create(hdx_site='test',
            #                                 user_agent_config_yaml=join(expanduser('~'), '.freshnessuseragent.yml'),
            #                                 project_config_yaml='src/hdx/freshness/project_configuration.yml')
            # logger.info('Site changed to: %s' % site_url)
            if self.testsession:
                serialize_datasets(self.testsession, self.datasets)
        else:
            self.datasets = datasets
        if now is None:  # pragma: no cover
            self.now = datetime.datetime.utcnow()
            if self.testsession:
                serialize_now(self.testsession, self.now)
        else:
            self.now = now
        dbrun = DBRun(run_number=self.run_number, run_date=self.now)
        self.session.add(dbrun)
        self.session.commit()

    def process_datasets(self, forced_hash_ids=None):
        self.datasets = list_distribute_contents(self.datasets, lambda x: x['organization']['name'])
        resources_to_check = list()
        datasets_to_check = dict()
        for dataset in self.datasets:
            if dataset.is_requestable():
                continue
            dataset_id = dataset['id']
            dict_of_lists_add(self.dataset_what_updated, 'total', dataset_id)
            organization_id = dataset['organization']['id']
            organization_name = dataset['organization']['name']
            organization_title = dataset['organization']['title']
            try:
                dborganization = self.session.query(DBOrganization).filter_by(id=organization_id).one()
                dborganization.name = organization_name
                dborganization.title = organization_title
            except NoResultFound:
                dborganization = DBOrganization(name=organization_name, id=organization_id, title=organization_title)
                self.session.add(dborganization)
            dataset_name = dataset['name']
            dataset_title = dataset['title']
            dataset_private = dataset['private']
            dataset_maintainer = dataset['maintainer']
            dataset_maintainer_email = dataset['maintainer_email']
            dataset_author = dataset['author']
            dataset_author_email = dataset['author_email']
            dataset_location = ','.join([x['name'] for x in dataset['groups']])
            try:
                dbinfodataset = self.session.query(DBInfoDataset).filter_by(id=dataset_id).one()
                dbinfodataset.name = dataset_name
                dbinfodataset.title = dataset_title
                dbinfodataset.private = dataset_private
                dbinfodataset.organization_id = organization_id
                dbinfodataset.maintainer = dataset_maintainer
                dbinfodataset.maintainer_email = dataset_maintainer_email
                dbinfodataset.author = dataset_author
                dbinfodataset.author_email = dataset_author_email
                dbinfodataset.location = dataset_location
            except NoResultFound:
                dbinfodataset = DBInfoDataset(name=dataset_name, id=dataset_id, title=dataset_title,
                                              private=dataset_private, organization_id=organization_id,
                                              maintainer=dataset_maintainer, maintainer_email=dataset_maintainer_email,
                                              author=dataset_author, author_email=dataset_author_email,
                                              location=dataset_location)
                self.session.add(dbinfodataset)
            try:
                previous_dbdataset = self.session.query(DBDataset).filter_by(run_number=self.previous_run_number,
                                                                             id=dataset_id).one()
            except NoResultFound:
                previous_dbdataset = None
            resources = dataset.get_resources()
            fresh = None
            dataset_resources, last_resource_updated, last_resource_modified = \
                self.process_resources(dataset_id, previous_dbdataset, resources, forced_hash_ids=forced_hash_ids)
            dataset_date = dataset.get('dataset_date')
            metadata_modified = parser.parse(dataset['metadata_modified'], ignoretz=True)
            update_frequency = dataset.get('data_update_frequency')
            if update_frequency is not None:
                update_frequency = int(update_frequency)
                if update_frequency == 0:
                    fresh = 0
                    self.live_update += 1
                elif update_frequency == -1:
                    fresh = 0
                    self.never_update += 1
                elif update_frequency == -2:
                    fresh = 0
                    self.adhoc_update += 1
                else:
                    fresh = self.calculate_aging(metadata_modified, update_frequency)
            dbdataset = DBDataset(run_number=self.run_number, id=dataset_id,
                                  dataset_date=dataset_date, update_frequency=update_frequency,
                                  metadata_modified=metadata_modified, last_modified=metadata_modified,
                                  what_updated='metadata', last_resource_updated=last_resource_updated,
                                  last_resource_modified=last_resource_modified, fresh=fresh, error=False)
            if previous_dbdataset is not None:
                if last_resource_modified <= previous_dbdataset.last_resource_modified:
                    dbdataset.last_resource_updated = previous_dbdataset.last_resource_updated
                    dbdataset.last_resource_modified = previous_dbdataset.last_resource_modified
                if metadata_modified <= previous_dbdataset.last_modified:
                    dbdataset.last_modified = previous_dbdataset.last_modified
                    dbdataset.what_updated = 'nothing'
                    if update_frequency is not None and update_frequency > 0:
                        fresh = self.calculate_aging(previous_dbdataset.last_modified, update_frequency)
                        dbdataset.fresh = fresh

            self.session.add(dbdataset)
            update_string = '%s, Updated %s' % (self.aging_statuses[fresh], dbdataset.what_updated)
            if fresh == 0 or update_frequency is None:
                hash_forced = False
                for url, resource_id, force_hash, what_updated in dataset_resources:
                    if force_hash:
                        hash_forced = True
                        resources_to_check.append((url, resource_id, True, what_updated))
                    else:
                        dict_of_lists_add(self.resource_what_updated, what_updated, resource_id)
                if hash_forced:
                    datasets_to_check[dataset_id] = update_string
                else:
                    dict_of_lists_add(self.dataset_what_updated, update_string, dataset_id)

            else:
                datasets_to_check[dataset_id] = update_string
                resources_to_check += dataset_resources
        self.session.commit()
        return datasets_to_check, resources_to_check

    def process_resources(self, dataset_id, previous_dbdataset, resources, forced_hash_ids=None):
        last_resource_updated = None
        last_resource_modified = None
        dataset_resources = list()
        for resource in resources:
            resource_id = resource['id']
            dict_of_lists_add(self.resource_what_updated, 'total', resource_id)
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
                    dbresource.when_hashed = previous_dbresource.when_hashed

                except NoResultFound:
                    pass
            self.session.add(dbresource)

            internal = False
            for url_substr in self.urls_internal:
                if url_substr in url:
                    self.internal_and_adhoc_what_updated(dbresource, 'internal')
                    internal = True
                    break
            ignore = False
            for url_substr in self.urls_adhoc_update:
                if url_substr in url:
                    self.internal_and_adhoc_what_updated(dbresource, 'adhoc')
                    ignore = True
                    break
            if forced_hash_ids:
                forced_hash = resource_id in forced_hash_ids
            else:
                forced_hash = self.urls_to_check_count < self.no_urls_to_check and \
                              (
                                      dbresource.when_checked is None or self.now - dbresource.when_checked > datetime.timedelta(
                                  days=30))
            if forced_hash:
                dataset_resources.append((url, resource_id, True, dbresource.what_updated))
                self.urls_to_check_count += 1
            elif internal or ignore:
                dict_of_lists_add(self.resource_what_updated, dbresource.what_updated, dbresource.id)
            else:
                dataset_resources.append((url, resource_id, False, dbresource.what_updated))
        return dataset_resources, last_resource_updated, last_resource_modified

    @staticmethod
    def internal_and_adhoc_what_updated(dbresource, url_substr):
        what_updated = '%s-%s' % (url_substr, dbresource.what_updated)
        dbresource.what_updated = what_updated

    def check_urls(self, resources_to_check, results=None, hash_results=None):
        def get_domain(x):
            return urlparse(x[0]).netloc
        if results is None:  # pragma: no cover
            resources_to_check = list_distribute_contents(resources_to_check, get_domain)
            results = retrieve(resources_to_check)
            if self.testsession:
                serialize_results(self.testsession, results)

        hash_check = list()
        for resource_id in results:
            url, err, http_last_modified, hash, force_hash = results[resource_id]
            if hash:
                dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                      run_number=self.run_number).one()
                if dbresource.md5_hash != hash:  # File changed
                    hash_check.append((url, resource_id, force_hash))

        if hash_results is None:  # pragma: no cover
            hash_check = list_distribute_contents(hash_check, get_domain)
            hash_results = retrieve(hash_check)
            if self.testsession:
                serialize_hashresults(self.testsession, hash_results)

        return results, hash_results

    def process_results(self, results, hash_results, resourcecls=Resource):
        datasets_lastmodified = dict()
        for resource_id in sorted(results):
            url, err, http_last_modified, hash, force_hash = results[resource_id]
            dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                  run_number=self.run_number).one()
            dataset_id = dbresource.dataset_id
            datasetinfo = datasets_lastmodified.get(dataset_id, dict())
            what_updated = dbresource.what_updated
            touch = False
            if http_last_modified:
                dbresource.http_last_modified = http_last_modified
                what_updated = self.set_last_modified(dbresource, dbresource.http_last_modified, 'http header')
                touch = True
            if hash:
                dbresource.when_checked = self.now
                dbresource.when_hashed = self.now
                if dbresource.md5_hash == hash:  # File unchanged
                    what_updated = self.add_what_updated(what_updated, 'same hash')
                else:  # File updated
                    prev_hash = dbresource.md5_hash
                    dbresource.md5_hash = hash
                    hash_url, hash_err, hash_http_last_modified, hash_hash, force_hash = hash_results[resource_id]
                    if hash_http_last_modified:
                        dbresource.http_last_modified = hash_http_last_modified
                        what_updated = self.set_last_modified(dbresource, dbresource.http_last_modified, 'http header')
                        touch = True
                    if hash_hash:
                        if hash_hash == hash:
                            if prev_hash is None:   # First occurrence of resource eg. first run - don't use hash
                                                    # for last modified field (and hence freshness calculation)
                                dbresource.what_updated = self.add_what_updated(what_updated, 'hash')
                                what_updated = dbresource.what_updated
                            else:
                                what_updated = self.set_last_modified(dbresource, self.now, 'hash')
                            touch = True
                            dbresource.api = False
                        else:
                            dbresource.md5_hash = hash_hash
                            what_updated = self.add_what_updated(what_updated, 'api')
                            dbresource.api = True
                    if hash_err:
                        what_updated = self.add_what_updated(what_updated, 'error')
                        dbresource.error = hash_err
            if err:
                dbresource.when_checked = self.now
                what_updated = self.add_what_updated(what_updated, 'error')
                dbresource.error = err
            datasetinfo[resource_id] = (dbresource.error, dbresource.last_modified, dbresource.what_updated)
            datasets_lastmodified[dataset_id] = datasetinfo
            dict_of_lists_add(self.resource_what_updated, what_updated, resource_id)
            # Uncomment if touch... for touching resources
            if touch:
                self.touch_count += 1
                # logger.info('Touch count: %d' % self.touch_count)
                # try:
                #     logger.info('Touching: %s' % resource_id)
                #     resource = resourcecls.read_from_hdx(resource_id)
                #     if resource:
                #         resource.touch()
                #     else:
                #         logger.error('Touching failed for %s! Resource does not exist.' % resource_id)
                # except HDXError:
                #     logger.exception('Touching failed for %s!' % resource_id)
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
                err, new_last_resource_modified, new_last_resource_what_updated = dataset[resource_id]
                if not err:
                    all_errors = False
                if new_last_resource_modified:
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
            if update_frequency is not None and update_frequency > 0:
                dbdataset.fresh = self.calculate_aging(dbdataset.last_modified, update_frequency)
            dbdataset.error = all_errors
            status = '%s, Updated %s' % (self.aging_statuses[dbdataset.fresh], dbdataset.what_updated)
            if all_errors:
                status = '%s,error' % status
            dict_of_lists_add(self.dataset_what_updated, status, dataset_id)
        self.session.commit()
        for dataset_id in datasets_to_check:
            if dataset_id in datasets_lastmodified:
                continue
            dict_of_lists_add(self.dataset_what_updated, datasets_to_check[dataset_id], dataset_id)

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
        output_str += '\n\n%d datasets have update frequency of Live' % self.live_update
        output_str += '\n%d datasets have update frequency of Never' % self.never_update
        output_str += '\n%d datasets have update frequency of Adhoc' % self.adhoc_update

        logger.info(output_str)
        return output_str

    def close(self):
        self.session.close()

    @staticmethod
    def set_last_modified(dbobject, modified_date, what_updated):
        if modified_date > dbobject.last_modified:
            dbobject.last_modified = modified_date
            dbobject.what_updated = DataFreshness.add_what_updated(dbobject.what_updated, what_updated)
        return dbobject.what_updated

    @staticmethod
    def add_what_updated(prev_what_updated, what_updated):
        if what_updated in prev_what_updated:
            return prev_what_updated
        if prev_what_updated != 'nothing':
            return '%s,%s' % (prev_what_updated, what_updated)
        else:
            return what_updated

    def calculate_aging(self, last_modified, update_frequency):
        delta = self.now - last_modified
        if delta >= self.aging[update_frequency]['Delinquent']:
            return 3
        elif delta >= self.aging[update_frequency]['Overdue']:
            return 2
        elif delta >= self.aging[update_frequency]['Due']:
            return 1
        return 0


