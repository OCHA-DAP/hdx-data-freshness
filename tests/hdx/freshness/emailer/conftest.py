"""Global fixtures"""

import os
import shutil
from os.path import join

import pytest

from hdx.api.configuration import Configuration


@pytest.fixture(scope="function")
def configuration():
    project_config_yaml = join(
        "tests", "fixtures", "emailer", "project_configuration.yaml"
    )
    Configuration._create(
        hdx_site="prod",
        user_agent="test",
        hdx_read_only=True,
        project_config_yaml=project_config_yaml,
    )
    return Configuration.read()


@pytest.fixture(scope="function")
def database_failure():
    dbfile = "test_freshness_failure.db"
    dbpath = join("tests", dbfile)
    try:
        os.remove(dbpath)
    except FileNotFoundError:
        pass
    shutil.copyfile(join("tests", "fixtures", "emailer", dbfile), dbpath)
    return {"dialect": "sqlite", "database": dbpath}
