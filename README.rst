HDX Data Freshness
~~~~~~~~~~~~~~~~~~

|Build Status| |Coverage Status|

The implementation of HDX freshness in Python reads all the datasets
from the `Humanitarian Data Exchange <http://data.humdata.org/>`__
website (using the HDX Python library) and then iterates through them
one by one performing a sequence of steps.

#. It gets the dataset's update frequency if it has one. If that update
   frequency is Never, then the dataset is always fresh.

#. If not, it checks if the dataset and resource metadata have changed -
   this qualifies as an update from a freshness perspective. It compares
   the difference between the current time and update time with the
   update frequency and sets a status: fresh, due, overdue or
   delinquent.

#. If the dataset is not fresh based on metadata, then the urls of the
   resources are examined. If they are internal urls (data.humdata.org -
   the HDX filestore, manage.hdx.rwlabs.org - CPS) then there is no
   further checking that can be done because when the files pointed to
   by these urls update, the HDX metadata is updated.

#. If they are urls with an adhoc update frequency
   (proxy.hxlstandard.org, ourairports.com), then freshness cannot be
   determined. Currently, there is no mechanism in HDX to specify adhoc
   update frequencies, but there is a proposal to add this to the update
   frequency options. At the moment, the freshness value for adhoc
   datasets is based on whatever has been set for update frequency, but
   these datasets can be easily identified and excluded from results if
   needed.

#. If the url is externally hosted and not adhoc, then we can open an
   HTTP GET request to the file and check the header returned for the
   Last-Modified field. If that field exists, then we read the date and
   time from it and check if that is more recent than the dataset or
   resource metadata modification date. If it is, we recalculate
   freshness.

#. If the resource is not fresh by this measure, then we download the
   file and calculate an MD5 hash for it. In our database, we store
   previous hash values, so we can check if the hash has changed since
   the last time we took the hash.

#. There are some resources where the hash changes constantly because
   they connect to an api which generates a file on the fly. To identify
   these, we hash again and check if the hash changes in the few seconds
   since the previous hash calculation.

Since there can be temporary connection and download issues with urls,
the code has multiple retry functionality with increasing delays. Also
as there are many requests to be made, rather than perform them one by
one, they are executed concurrently using the asynchronous functionality
that has been added to the most recent versions of Python.

Usage
~~~~~

::

    python run.py

.. |Build Status| image:: https://travis-ci.org/OCHA-DAP/hdx-data-freshness.svg?branch=master&ts=1
   :target: https://travis-ci.org/OCHA-DAP/hdx-data-freshness
.. |Coverage Status| image:: https://coveralls.io/repos/github/OCHA-DAP/hdx-data-freshness/badge.svg?branch=master&ts=1
   :target: https://coveralls.io/github/OCHA-DAP/hdx-data-freshness?branch=master
