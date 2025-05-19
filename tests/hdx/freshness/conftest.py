"""Global fixtures"""

from collections import UserDict

import pytest

from hdx.api.configuration import Configuration
from hdx.freshness.app.__main__ import main
from hdx.utilities.path import script_dir_plus_file


@pytest.fixture(scope="session")
def configuration():
    project_config_yaml = script_dir_plus_file("project_configuration.yaml", main)
    Configuration._create(
        hdx_site="prod",
        user_agent="test",
        hdx_read_only=True,
        project_config_yaml=project_config_yaml,
    )
    return Configuration.read()


@pytest.fixture(scope="session")
def resourcecls():
    class MyResource(UserDict):
        touched = False
        broken = False
        resourcedict = None

        def __init__(self, id):
            self.data = self.resourcedict[id]

        @classmethod
        def populate_resourcedict(cls, datasets):
            cls.resourcedict = dict()
            for dataset in datasets:
                for resource in dataset.get_resources():
                    cls.resourcedict[resource["id"]] = resource

        @staticmethod
        def read_from_hdx(id):
            return MyResource(id)

        @classmethod
        def update_in_hdx(cls, operation, batch_mode, skip_validation, ignore_check):
            cls.touched = True

        @classmethod
        def mark_broken(cls):
            cls.broken = True

    return MyResource
