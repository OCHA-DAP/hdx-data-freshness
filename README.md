[![Build Status](https://github.com/OCHA-DAP/hdx-data-freshness/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-data-freshness/actions?query=workflow%3Abuild)
[![Coverage Status](https://codecov.io/gh/OCHA-DAP/hdx-data-freshness/branch/main/graph/badge.svg?token=JpWZc5js4y)](https://codecov.io/gh/OCHA-DAP/hdx-data-freshness)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

HDX data freshness is a process that runs against the [Humanitarian Data Exchange](https://data.humdata.org/)
portal every day. It attempts to determine the freshness of datasets on the platform (ie. how up to date they
are) and updates the last modified field of resources held externally to HDX that have changed.

For more information, please read the [documentation](https://hdx-data-freshness.readthedocs.io/en/latest/). 

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.
