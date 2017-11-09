# -*- coding: UTF-8 -*-
"""Global fixtures"""
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration


@pytest.fixture(scope='session')
def configuration():
    project_config_yaml = join('src', 'hdx', 'freshness', 'project_configuration.yml')
    Configuration._create(hdx_site='prod', hdx_read_only=True, project_config_yaml=project_config_yaml)


@pytest.fixture(scope='session')
def resourcecls():
    class MyResource:
        @staticmethod
        def read_from_hdx(id):
            return MyResource()

        @staticmethod
        def patch():
            pass

    return MyResource
