# -*- coding: utf-8 -*-
'''
Data freshness:
--------------

Calculate freshness for all datasets in HDX.

'''
import datetime
import logging
import re
from parser import ParserError
from urllib.parse import urlparse

from dateutil import parser
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.hdx_configuration import Configuration
from hdx.utilities.dictandlist import dict_of_lists_add, list_distribute_contents
from sqlalchemy import exists, and_
from sqlalchemy.orm.exc import NoResultFound

from hdx.freshness.database.dbdataset import DBDataset
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dborganization import DBOrganization
from hdx.freshness.database.dbresource import DBResource
from hdx.freshness.database.dbrun import DBRun
from hdx.freshness.retrieval import retrieve
from hdx.freshness.testdata.serialize import serialize_datasets, serialize_now, serialize_results, serialize_hashresults

logger = logging.getLogger(__name__)

default_no_urls_to_check = 1000


class DataFreshness:
    bracketed_date = re.compile(r'\((.*)\)')

    def __init__(self, session=None, testsession=None, datasets=None, now=None, do_touch=False):
        ''''''
        self.session = session
        self.urls_to_check_count = 0
        self.never_update = 0
        self.live_update = 0
        self.adhoc_update = 0
        self.dataset_what_updated = dict()
        self.resource_what_updated = dict()
        self.resource_last_modified_count = 0
        self.do_touch = do_touch

        self.url_internal = 'data.humdata.org'

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
        self.testsession = testsession
        if datasets is None:  # pragma: no cover
            Configuration.read().set_read_only(True)  # so that we only get public datasets
            logger.info('Retrieving all datasets from HDX')
            self.datasets = Dataset.get_all_datasets()
            Configuration.read().set_read_only(False)
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
        self.previous_run_number = self.session.query(DBRun.run_number).distinct().order_by(
            DBRun.run_number.desc()).first()
        if self.previous_run_number is not None:
            self.previous_run_number = self.previous_run_number[0]
            self.run_number = self.previous_run_number + 1
            no_resources = self.no_resources_force_hash()
            if no_resources:
                self.no_urls_to_check = int((no_resources / 30) + 1)
            else:
                self.no_urls_to_check = default_no_urls_to_check
        else:
            self.previous_run_number = None
            self.run_number = 0
            self.no_urls_to_check = default_no_urls_to_check

        logger.info('Will force hash %d resources' % self.no_urls_to_check)

    def no_resources_force_hash(self):
        columns = [DBResource.id, DBDataset.updated_by_script]
        filters = [DBResource.dataset_id == DBDataset.id, DBResource.run_number == self.previous_run_number,
                   DBDataset.run_number == self.previous_run_number,
                   DBResource.url.notlike('%{}%'.format(self.url_internal))]
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

    def spread_datasets(self):
        self.datasets = list_distribute_contents(self.datasets, lambda x: x['organization']['name'])

    def add_new_run(self):
        dbrun = DBRun(run_number=self.run_number, run_date=self.now)
        self.session.add(dbrun)
        self.session.commit()

    def process_datasets(self, forced_hash_ids=None):
        resources_to_check = list()
        datasets_to_check = dict()
        logger.info('Processing datasets')
        for dataset in self.datasets:
            resources = dataset.get_resources()
            if dataset.is_requestable():  # ignore requestable
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
            dataset_location = ','.join([x['name'] for x in dataset['groups']])
            try:
                dbinfodataset = self.session.query(DBInfoDataset).filter_by(id=dataset_id).one()
                dbinfodataset.name = dataset_name
                dbinfodataset.title = dataset_title
                dbinfodataset.private = dataset_private
                dbinfodataset.organization_id = organization_id
                dbinfodataset.maintainer = dataset_maintainer
                dbinfodataset.location = dataset_location
            except NoResultFound:
                dbinfodataset = DBInfoDataset(name=dataset_name, id=dataset_id, title=dataset_title,
                                              private=dataset_private, organization_id=organization_id,
                                              maintainer=dataset_maintainer, location=dataset_location)
                self.session.add(dbinfodataset)
            try:
                previous_dbdataset = self.session.query(DBDataset).filter_by(run_number=self.previous_run_number,
                                                                             id=dataset_id).one()
            except NoResultFound:
                previous_dbdataset = None
            dont_hash_script_update = False
            update_frequency = dataset.get('data_update_frequency')
            updated_by_script = dataset.get('updated_by_script')
            if update_frequency is not None:
                update_frequency = int(update_frequency)
                if updated_by_script:
                    if 'freshness_ignore' in updated_by_script:
                        updated_by_script = None
                    else:
                        match = self.bracketed_date.search(updated_by_script)
                        if match is None:
                            updated_by_script = None
                        else:
                            try:
                                updated_by_script = parser.parse(match.group(1), ignoretz=True)
                                dont_hash_script_update = True
                            except ParserError:
                                updated_by_script = None
            dataset_resources, last_resource_updated, last_resource_modified = \
                self.process_resources(dataset_id, previous_dbdataset, resources, dont_hash_script_update,
                                       forced_hash_ids=forced_hash_ids)
            dataset_date = dataset.get('dataset_date')
            metadata_modified = parser.parse(dataset['metadata_modified'], ignoretz=True)
            if 'last_modified' in dataset:
                last_modified = parser.parse(dataset['last_modified'], ignoretz=True)
            else:
                last_modified = datetime.datetime(1970, 1, 1, 0, 0)
            if len(resources) == 0 and last_resource_updated is None:
                last_resource_updated = 'NO RESOURCES'
                last_resource_modified = datetime.datetime(1970, 1, 1, 0, 0)
                error = True
                what_updated = 'no resources'
            else:
                error = False
                what_updated = 'firstrun'
            review_date = dataset.get('review_date')
            if review_date is None:
                latest_of_modifieds = last_modified
            else:
                review_date = parser.parse(review_date, ignoretz=True)
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
                    self.adhoc_update += 1
                else:
                    fresh = self.calculate_aging(latest_of_modifieds, update_frequency)
            dbdataset = DBDataset(run_number=self.run_number, id=dataset_id,
                                  dataset_date=dataset_date, update_frequency=update_frequency,
                                  review_date=review_date, last_modified=last_modified,
                                  metadata_modified=metadata_modified, updated_by_script=updated_by_script,
                                  latest_of_modifieds=latest_of_modifieds, what_updated=what_updated,
                                  last_resource_updated=last_resource_updated,
                                  last_resource_modified=last_resource_modified, fresh=fresh, error=error)
            if previous_dbdataset is not None and not error:
                dbdataset.what_updated = self.add_what_updated(dbdataset.what_updated, 'nothing')
                if last_modified > previous_dbdataset.last_modified:  # filestore update would cause this
                    dbdataset.what_updated = self.add_what_updated(dbdataset.what_updated, 'filestore')
                else:
                    dbdataset.last_modified = previous_dbdataset.last_modified
                if previous_dbdataset.review_date is None:
                    if review_date is not None:
                        dbdataset.what_updated = self.add_what_updated(dbdataset.what_updated, 'review date')
                else:
                    if review_date is not None and review_date > previous_dbdataset.review_date:  # someone clicked the review button
                        dbdataset.what_updated = self.add_what_updated(dbdataset.what_updated, 'review date')
                    else:
                        dbdataset.review_date = previous_dbdataset.review_date
                if updated_by_script and (
                        previous_dbdataset.updated_by_script is None or updated_by_script > previous_dbdataset.updated_by_script):  # new script update of datasets
                    dbdataset.what_updated = self.add_what_updated(dbdataset.what_updated, 'script update')
                else:
                    dbdataset.updated_by_script = previous_dbdataset.updated_by_script
                if last_resource_modified <= previous_dbdataset.last_resource_modified:
                    # we keep this so that although we don't normally use it,
                    # we retain the ability to run without touching CKAN
                    dbdataset.last_resource_updated = previous_dbdataset.last_resource_updated
                    dbdataset.last_resource_modified = previous_dbdataset.last_resource_modified
                if latest_of_modifieds < previous_dbdataset.latest_of_modifieds:
                    dbdataset.latest_of_modifieds = previous_dbdataset.latest_of_modifieds
                    if update_frequency is not None and update_frequency > 0:
                        fresh = self.calculate_aging(previous_dbdataset.latest_of_modifieds, update_frequency)
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

    def process_resources(self, dataset_id, previous_dbdataset, resources, dont_hash_script_update,
                          forced_hash_ids=None):
        last_resource_updated = None
        last_resource_modified = None
        dataset_resources = list()
        for resource in resources:
            resource_id = resource['id']
            dict_of_lists_add(self.resource_what_updated, 'total', resource_id)
            url = resource['url']
            name = resource['name']
            revision_last_updated = parser.parse(resource['revision_last_updated'], ignoretz=True)
            last_modified = parser.parse(resource['last_modified'], ignoretz=True)
            if last_resource_modified:
                if last_modified > last_resource_modified:
                    last_resource_updated = resource_id
                    last_resource_modified = last_modified
            else:
                last_resource_updated = resource_id
                last_resource_modified = last_modified
            dbresource = DBResource(run_number=self.run_number, id=resource_id, name=name,
                                    dataset_id=dataset_id, url=url, last_modified=last_modified,
                                    revision_last_updated=revision_last_updated, latest_of_modifieds=last_modified,
                                    what_updated='firstrun')
            if previous_dbdataset is not None:
                try:
                    previous_dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                                   run_number=
                                                                                   previous_dbdataset.run_number).one()
                    if last_modified > previous_dbresource.last_modified:
                        dbresource.what_updated = 'filestore'
                    else:
                        dbresource.last_modified = previous_dbresource.last_modified
                        dbresource.what_updated = 'nothing'
                    if last_modified <= previous_dbresource.latest_of_modifieds:
                        dbresource.latest_of_modifieds = previous_dbresource.latest_of_modifieds
                    dbresource.http_last_modified = previous_dbresource.http_last_modified
                    dbresource.md5_hash = previous_dbresource.md5_hash
                    dbresource.when_hashed = previous_dbresource.when_hashed
                    dbresource.when_checked = previous_dbresource.when_checked

                except NoResultFound:
                    pass
            self.session.add(dbresource)

            if self.url_internal in url:
                self.internal_what_updated(dbresource, 'internal')
                dict_of_lists_add(self.resource_what_updated, dbresource.what_updated, dbresource.id)
                continue
            if dont_hash_script_update:
                forced_hash = False
            else:
                if forced_hash_ids:
                    forced_hash = resource_id in forced_hash_ids
                else:
                    forced_hash = self.urls_to_check_count < self.no_urls_to_check \
                                  and (dbresource.when_checked is None or
                                       self.now - dbresource.when_checked > datetime.timedelta(days=30))
            if forced_hash:
                dataset_resources.append((url, resource_id, True, dbresource.what_updated))
                self.urls_to_check_count += 1
            else:
                dataset_resources.append((url, resource_id, False, dbresource.what_updated))
        return dataset_resources, last_resource_updated, last_resource_modified

    @staticmethod
    def internal_what_updated(dbresource, url_substr):
        what_updated = '%s-%s' % (url_substr, dbresource.what_updated)
        dbresource.what_updated = what_updated

    def check_urls(self, resources_to_check, user_agent, results=None, hash_results=None):
        def get_domain(x):
            return urlparse(x[0]).netloc
        if results is None:  # pragma: no cover
            resources_to_check = list_distribute_contents(resources_to_check, get_domain)
            results = retrieve(resources_to_check, user_agent)
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
            hash_results = retrieve(hash_check, user_agent)
            if self.testsession:
                serialize_hashresults(self.testsession, hash_results)

        return results, hash_results

    def process_results(self, results, hash_results, resourcecls=Resource):
        datasets_latest_of_modifieds = dict()
        for resource_id in sorted(results):
            url, err, http_last_modified, hash, force_hash = results[resource_id]
            dbresource = self.session.query(DBResource).filter_by(id=resource_id,
                                                                  run_number=self.run_number).one()
            dataset_id = dbresource.dataset_id
            datasetinfo = datasets_latest_of_modifieds.get(dataset_id, dict())
            what_updated = dbresource.what_updated
            update_last_modified = False
            if http_last_modified:
                if dbresource.http_last_modified is None or http_last_modified > dbresource.http_last_modified:
                    dbresource.http_last_modified = http_last_modified
            if hash:
                dbresource.when_checked = self.now
                dbresource.when_hashed = self.now
                if dbresource.md5_hash == hash:  # File unchanged
                    what_updated = self.add_what_updated(what_updated, 'same hash')
                else:  # File updated
                    hash_to_set = hash
                    hash_url, hash_err, hash_http_last_modified, hash_hash, force_hash = hash_results[resource_id]
                    if hash_http_last_modified:
                        if dbresource.http_last_modified is None or hash_http_last_modified > dbresource.http_last_modified:
                            dbresource.http_last_modified = hash_http_last_modified
                    if hash_hash:
                        if hash_hash == hash:
                            if dbresource.md5_hash is None:  # First occurrence of resource eg. first run - don't use hash
                                                    # for last modified field (and hence freshness calculation)
                                dbresource.what_updated = self.add_what_updated(what_updated, 'hash')
                                what_updated = dbresource.what_updated
                            else:
                                # Check if hash has occurred before
                                # select distinct md5_hash from dbresources where id = '714ef7b5-a303-4e4f-be2f-03b2ce2933c7' and md5_hash='2f3cd6a6fce5ad4d7001780846ad87a7';
                                if self.session.query(exists().where(
                                        and_(DBResource.id == resource_id, DBResource.md5_hash == hash))).scalar():
                                    dbresource.what_updated = self.add_what_updated(what_updated, 'repeat hash')
                                    what_updated = dbresource.what_updated
                                else:
                                    what_updated, _ = self.set_latest_of_modifieds(dbresource, self.now, 'hash')
                                    update_last_modified = True
                            dbresource.api = False
                        else:
                            hash_to_set = hash_hash
                            what_updated = self.add_what_updated(what_updated, 'api')
                            dbresource.api = True
                    if hash_err:
                        what_updated = self.add_what_updated(what_updated, 'error')
                        dbresource.error = hash_err
                    dbresource.md5_hash = hash_to_set
            if err:
                dbresource.when_checked = self.now
                what_updated = self.add_what_updated(what_updated, 'error')
                dbresource.error = err
            datasetinfo[resource_id] = (dbresource.error, dbresource.latest_of_modifieds, dbresource.what_updated)
            datasets_latest_of_modifieds[dataset_id] = datasetinfo
            dict_of_lists_add(self.resource_what_updated, what_updated, resource_id)
            if update_last_modified and self.do_touch:
                try:
                    logger.info('Updating last modified for resource %s' % resource_id)
                    resource = resourcecls.read_from_hdx(resource_id)
                    if resource:
                        last_modified = parser.parse(resource['last_modified'])
                        dbdataset = self.session.query(DBDataset).filter_by(id=dataset_id,
                                                                            run_number=self.run_number).one()
                        update_frequency = dbdataset.update_frequency
                        if update_frequency > 0:
                            if self.calculate_aging(last_modified, update_frequency) == 0:
                                dotouch = False
                            else:
                                dotouch = True
                        else:
                            dotouch = True
                        if dotouch:
                            self.resource_last_modified_count += 1
                            logger.info('Resource last modified count: %d' % self.resource_last_modified_count)
                            resource['last_modified'] = dbresource.latest_of_modifieds.isoformat()
                            resource.update_in_hdx(operation='patch', batch_mode='KEEP_OLD', skip_validation=True,
                                                   ignore_check=True)
                        else:
                            logger.info("Didn't update last modified for resource %s as it is fresh!" % resource_id)
                    else:
                        logger.error('Last modified update failed for id %s! Resource does not exist.' % resource_id)
                except HDXError:
                    logger.exception('Last modified update failed for id %s!' % resource_id)
        self.session.commit()
        return datasets_latest_of_modifieds

    def update_dataset_latest_of_modifieds(self, datasets_to_check, datasets_latest_of_modifieds):
        for dataset_id in datasets_latest_of_modifieds:
            dbdataset = self.session.query(DBDataset).filter_by(id=dataset_id,
                                                                run_number=self.run_number).one()
            dataset = datasets_latest_of_modifieds[dataset_id]
            dataset_latest_of_modifieds = dbdataset.latest_of_modifieds
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
                    if new_last_resource_modified > dataset_latest_of_modifieds:
                        dataset_latest_of_modifieds = new_last_resource_modified
                        dataset_what_updated = new_last_resource_what_updated
            dbdataset.last_resource_updated = last_resource_updated
            dbdataset.last_resource_modified = last_resource_modified
            self.set_latest_of_modifieds(dbdataset, dataset_latest_of_modifieds, dataset_what_updated)
            update_frequency = dbdataset.update_frequency
            if update_frequency is not None and update_frequency > 0:
                dbdataset.fresh = self.calculate_aging(dbdataset.latest_of_modifieds, update_frequency)
            dbdataset.error = all_errors
            status = '%s, Updated %s' % (self.aging_statuses[dbdataset.fresh], dbdataset.what_updated)
            if all_errors:
                status = '%s,error' % status
            dict_of_lists_add(self.dataset_what_updated, status, dataset_id)
        self.session.commit()
        for dataset_id in datasets_to_check:
            if dataset_id in datasets_latest_of_modifieds:
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

    @staticmethod
    def set_latest_of_modifieds(dbobject, modified_date, what_updated):
        if modified_date > dbobject.latest_of_modifieds:
            dbobject.latest_of_modifieds = modified_date
            dbobject.what_updated = DataFreshness.add_what_updated(dbobject.what_updated, what_updated)
            update = True
        else:
            update = False
        return dbobject.what_updated, update

    @staticmethod
    def add_what_updated(prev_what_updated, what_updated):
        if what_updated in prev_what_updated:
            return prev_what_updated
        if prev_what_updated != 'nothing' and prev_what_updated != 'firstrun':
            if what_updated != 'nothing':
                return '%s,%s' % (prev_what_updated, what_updated)
            return prev_what_updated
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


