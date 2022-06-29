# Summary

HDX data freshness is a process that runs against the [Humanitarian Data Exchange](https://data.humdata.org/)
portal every day. It attempts to determine the freshness of datasets on the platform (ie. how up to date they
are) and updates the last modified field of resources held externally to HDX that have changed.

# Information

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-data-freshness).
The library has detailed API documentation which can be found in the menu at the top. 

# Description

The implementation of HDX freshness in Python reads all the datasets
from the [Humanitarian Data Exchange](http://data.humdata.org/) website
(using the HDX Python library) and then iterates through them one by one
performing a sequence of steps:

1. It gets the dataset's update frequency if it has one. If that update
frequency is Never, As Needed or Live then the dataset is always fresh.
2. If not, it checks if the dataset and resource metadata have changed
    -this qualifies as an update from a freshness perspective. It
    compares the difference between the current time and update time
    with the update frequency and sets a status: fresh, due, overdue or
    delinquent.
3. If the dataset is not fresh based on metadata, then the urls of the
    resources are examined. If they are internal urls (data.humdata.org
    -the HDX filestore, manage.hdx.rwlabs.org - CPS) then there is no
    further checking that can be done because when the files pointed to
    by these urls update, the HDX metadata is updated.
4. If the url is externally hosted, then we can open an
    HTTP GET request to the file and check the header returned for the
    Last-Modified field. If that field exists, then we read the date and
    time from it and check if that is more recent than the dataset or
    resource metadata modification date. If it is, we recalculate
    freshness.
5. If the resource is not fresh by this measure, then we download the
    file and calculate an MD5 hash for it. In our database, we store
    previous hash values, so we can check if the hash has changed since
    the last time we took the hash. For xlsx, if the hash is constantly 
    changing, we hash the individual sheets in the workbook.
6. There are some resources where the hash changes constantly because
    they connect to an api which generates a file on the fly. To
    identify these, we hash again and check if the hash changes in the
    few seconds since the previous hash calculation.

Since there can be temporary connection and download issues with urls,
the code has multiple retry functionality with increasing delays. Also
as there are many requests to be made, rather than perform them one by
one, they are executed concurrently using the asynchronous functionality
(asyncio) available in Python.

# Usage

    python run.py
