# -*- coding: UTF-8 -*-
"""Global fixtures"""
from collections import UserDict
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration


@pytest.fixture(scope='session')
def configuration():
    project_config_yaml = join('src', 'hdx', 'freshness', 'project_configuration.yml')
    Configuration._create(hdx_site='prod', user_agent='test', hdx_read_only=True,
                          project_config_yaml=project_config_yaml)


@pytest.fixture(scope='session')
def resourcecls():
    class MyResource(UserDict, object):
        def __init__(self):
            self.data = {'a': '1'}

        @staticmethod
        def read_from_hdx(id):
            return MyResource()

        @staticmethod
        def update_in_hdx(operation, batch_mode, skip_validation):
            pass

    return MyResource
