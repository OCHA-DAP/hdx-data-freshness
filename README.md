[![Build Status](https://github.com/OCHA-DAP/hdx-data-freshness/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-data-freshness/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-data-freshness/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-data-freshness?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

HDX data freshness is a process that runs against the [Humanitarian Data Exchange](https://data.humdata.org/)
portal every day. It attempts to determine the freshness of datasets on the platform (ie. how up to date they
are) and updates the last modified field of resources held externally to HDX that have changed.

For more information, please read the [documentation](https://hdx-data-freshness.readthedocs.io/en/latest/).

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have
humanitarian related data, please upload your datasets to HDX.
