# -*- coding: UTF-8 -*-
"""Global fixtures"""
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration


@pytest.fixture(scope='function')
def configuration():
    project_config_yaml = join('src', 'freshness', 'project_configuration.yml')
    Configuration._create(hdx_site='prod', hdx_read_only=True, project_config_yaml=project_config_yaml)

