# -*- coding: utf-8 -*-
'''
Unit tests for the freshness class.

'''
import json
import os
import random
from datetime import datetime, timedelta
from os.path import join

import pygsheets
import pytest
from google.oauth2 import service_account
from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration
from hdx.utilities.database import Database

from hdx.freshness.datafreshness import DataFreshness


class TestFreshnessCKAN:
    @pytest.fixture(scope='class')
    def configuration(self):
        project_config_yaml = join('src', 'hdx', 'freshness', 'project_configuration.yml')
        hdx_key = os.getenv('HDX_KEY')
        Configuration._create(hdx_site='feature', user_agent='test', hdx_key=hdx_key,
                              project_config_yaml=project_config_yaml)

    @pytest.fixture(scope='function')
    def datasetmetadata(self):
        return join('tests', 'fixtures', 'CKAN', 'hdx_dataset_static.yml')

    @pytest.fixture(scope='function')
    def nodatabase(self):
        dbpath = join('tests', 'test_freshness.db')
        try:
            os.remove(dbpath)
        except FileNotFoundError:
            pass
        return {'driver': 'sqlite', 'database': dbpath}

    @pytest.fixture(scope='function')
    def gclient(self):
        gsheet_auth = os.getenv('GSHEET_AUTH')
        if not gsheet_auth:
            return None
        try:
            info = json.loads(gsheet_auth)
            scopes = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
            credentials = service_account.Credentials.from_service_account_info(info, scopes=scopes)
            return pygsheets.authorize(custom_credentials=credentials)
        except Exception:
            return None

    @pytest.fixture(scope='function')
    def setup_teardown_folder(self, gclient):
        body = {
            'name': 'freshness_test_tmp',
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': ['1y9jK4EwBb9SszbSexjIF8c6rzIjoN8xh']
        }
        folderid = gclient.drive.service.files().create(body=body).execute()['id']
        yield gclient, folderid
        gclient.drive.delete(folderid)

    def test_generate_dataset(self, configuration, datasetmetadata, nodatabase, setup_teardown_folder):
        today = datetime.now()
        gclient, folderid = setup_teardown_folder

        unchanging_gsheet = gclient.create('unchanging', folder=folderid)
        unchanging_gsheet.share('', role='reader', type='anyone')
        wks = unchanging_gsheet.sheet1
        # update the sheet with array
        wks.update_values('A1', [[random.random() for i in range(4)] for j in range(3)])
        unchanging_url = '%s/export?format=csv' % unchanging_gsheet.url

        changing_gsheet1 = gclient.create('changing1', folder=folderid)
        changing_gsheet1.share('', role='reader', type='anyone')
        changing_wks1 = changing_gsheet1.sheet1
        # update the sheet with array
        changing_wks1.update_values('A1', [[random.random() for i in range(5)] for j in range(2)])
        changing_url1 = '%s/export?format=csv' % changing_gsheet1.url

        changing_gsheet2 = gclient.create('changing2', folder=folderid)
        changing_gsheet2.share('', role='reader', type='anyone')
        changing_wks2 = changing_gsheet2.sheet1
        # update the sheet with array
        changing_wks2.update_values('A1', [[random.random() for i in range(3)] for j in range(6)])
        changing_url2 = '%s/export?format=csv' % changing_gsheet2.url

        datasets = list()
        last_modifieds = list()
        fresh = datetime.utcnow() - timedelta(days=1)
        due = fresh - timedelta(days=8)
        days7 = timedelta(days=7)
        overdue = due - days7
        delinquent = overdue - days7
        fresh = fresh.isoformat()
        due = due.isoformat()
        overdue = overdue.isoformat()
        delinquent = delinquent.isoformat()
        for i in range(7):
            dataset = Dataset({
                'name': 'freshness_test_%d' % i,
                'title': 'freshness test %d' % i
            })
            dataset.update_from_yaml(datasetmetadata)
            dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
            dataset.set_organization('5a63012e-6c41-420c-8c33-e84b277fdc90')
            dataset.set_dataset_date_from_datetime(today)
            if i == 6:
                dataset.set_expected_update_frequency('Never')
            else:
                dataset.set_expected_update_frequency('Every week')

            dataset.set_subnational(True)
            dataset.add_country_location('AFG')
            tags = ['protests']
            dataset.add_tags(tags)
            resource = {
                'name': 'test_resource_%d' % i,
                'description': 'Test Resource %d' % i,
                'format': 'csv',
                'url': unchanging_url
            }
            switcher = {0: (unchanging_url, fresh),
                        1: (changing_url1, overdue),
                        2: (unchanging_url, delinquent),
                        3: (unchanging_url, due),
                        4: (changing_url2, fresh),
                        5: (unchanging_url, overdue),
                        6: (unchanging_url, delinquent)}
            resource['url'], resource['last_modified'] = switcher.get(i)
            dataset.add_update_resource(resource)
            # add resources
            dataset.create_in_hdx()
            datasets.append(dataset)
            last_modifieds.append({'start': dataset['last_modified']})
        try:
            with Database(**nodatabase) as session:
                # first run
                freshness = DataFreshness(session=session, datasets=datasets, do_touch=True)
                freshness.spread_datasets()
                freshness.add_new_run()
                forced_hash_ids = [datasets[3].get_resource()['id'], datasets[4].get_resource()['id']]
                datasets_to_check, resources_to_check = freshness.process_datasets(forced_hash_ids=forced_hash_ids)
                results, hash_results = freshness.check_urls(resources_to_check, 'test')
                datasets_lastmodified = freshness.process_results(results, hash_results)
                freshness.update_dataset_latest_of_modifieds(datasets_to_check, datasets_lastmodified)
                run1_last_modified = freshness.now.isoformat()
                output1 = freshness.output_counts()
                # change something
                changing_wks1.update_values('A1', [[random.random() for i in range(5)] for j in range(2)])
                changing_wks2.update_values('A1', [[random.random() for i in range(3)] for j in range(6)])
                # second run
                for i, dataset in enumerate(datasets):
                    dataset = Dataset.read_from_hdx(dataset['id'])
                    last_modifieds[i]['run1'] = dataset['last_modified']
                    if i == 5:
                        dataset['review_date'] = due
                    datasets[i] = dataset
                freshness = DataFreshness(session=session, datasets=datasets, do_touch=True)
                freshness.spread_datasets()
                freshness.add_new_run()
                datasets_to_check, resources_to_check = freshness.process_datasets(forced_hash_ids=forced_hash_ids)
                results, hash_results = freshness.check_urls(resources_to_check, 'test')
                datasets_lastmodified = freshness.process_results(results, hash_results)
                freshness.update_dataset_latest_of_modifieds(datasets_to_check, datasets_lastmodified)
                run2_last_modified = freshness.now.isoformat()
                output2 = freshness.output_counts()
        finally:
            # tear down
            for i, dataset in enumerate(datasets):
                dataset = Dataset.read_from_hdx(dataset['id'])
                last_modifieds[i]['run2'] = dataset['last_modified']
                dataset.delete_from_hdx()

        assert output1 == '''
*** Resources ***
* total: 7 *,
firstrun: 2,
hash: 5

*** Datasets ***
* total: 7 *,
0: Fresh, Updated firstrun: 3,
1: Due, Updated firstrun: 1,
2: Overdue, Updated firstrun: 2,
3: Delinquent, Updated firstrun: 1

0 datasets have update frequency of Live
1 datasets have update frequency of Never
0 datasets have update frequency of Adhoc'''
        assert output2 == '''
*** Resources ***
* total: 7 *,
hash: 2,
nothing: 2,
same hash: 3

*** Datasets ***
* total: 7 *,
0: Fresh, Updated hash: 2,
0: Fresh, Updated nothing: 2,
1: Due, Updated nothing: 1,
1: Due, Updated review date: 1,
3: Delinquent, Updated nothing: 1

0 datasets have update frequency of Live
1 datasets have update frequency of Never
0 datasets have update frequency of Adhoc'''

        assert last_modifieds == [{'start': fresh, 'run1': fresh, 'run2': fresh},
                                  {'start': overdue, 'run1': overdue, 'run2': run2_last_modified},
                                  {'start': delinquent, 'run1': delinquent, 'run2': delinquent},
                                  {'start': due, 'run1': due, 'run2': due},
                                  {'start': fresh, 'run1': fresh, 'run2': fresh},
                                  {'start': overdue, 'run1': overdue, 'run2': overdue},
                                  {'start': delinquent, 'run1': delinquent, 'run2': delinquent}]
