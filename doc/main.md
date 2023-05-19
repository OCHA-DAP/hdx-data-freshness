# Summary

HDX data freshness is a process that runs against the [Humanitarian Data Exchange](https://data.humdata.org/)
portal every day. It attempts to determine the freshness of datasets on the platform (ie. how up to date they
are) and updates the last modified field of resources held externally to HDX that have changed.

The freshness emailer sends emails to HDX contributors and admins depending 
upon the freshness and integrity of datasets.

The freshness cleaner removes older freshness database runs.

# Information

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-data-freshness).
The library has detailed API documentation which can be found in the menu at the top. 

# Description

## Freshness

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

## Emailer

The HDX freshness emailer reads the HDX data freshness database and finds datasets whose status has changed. 
It sends emails to system administrators if the change is from overdue to delinquent or to maintainers if 
from due to overdue. It also alerts when there are new candidates for the data grid and reports datasets that 
are broken or which have invalid maintainers. An email is also sent for organisations with invalid 
administrators. 

## DBActions

Utilities to:
1. Clean the freshness database
2. Make a shallow clone of the freshness database

The cleaning action reduces the size of the database by removing runs 
according to these rules:
1. Keep a handful of runs around the end of each quarter all the way back to 
the first run in 2017
2. Keep daily runs going back 2 years
3. Keep weekly runs from 2 to 4 years back
4. Keep monthly runs for 4 years back and earlier

The cloning action creates a shallow clone of the database which has all the 
runs but only one dataset and its resources per run for testing purposes.

# Usage

In the command line usage examples below, common parameters are set as follows:

Either db_uri or db_params must be provided or the environment variable DB_URI
must be set. db_uri or DB_URI are of form: 
`postgresql+psycopg://user:password@host:port/database`

db_params is of form:
`database=XXX,host=X.X.X.X,username=XXX,password=XXX,port=1234,
ssh_host=X.X.X.X,ssh_port=1234,ssh_username=XXX,
ssh_private_key=/home/XXX/.ssh/keyfile`

## Freshness

    python -m hdx.freshness.app PARAMETERS

The PARAMETERS are:

    -hk HDX_KEY, --hdx_key HDX_KEY
                        HDX api key
    -ua USER_AGENT, --user_agent USER_AGENT
                        user agent
    -hs HDX_SITE, --hdx_site HDX_SITE
                        HDX site to use
    -db DB_URI, --db_uri DB_URI
                        Database connection string
    -dp DB_PARAMS, --db_params DB_PARAMS
                        Database connection parameters. Overrides --db_uri.
    -dt, --donttouch      
                        Don't touch datasets
    -s, --save            
                        Save state for testing


## Emailer

    python -m hdx.freshness.emailer.app PARAMETERS

The PARAMETERS are:

    -hk HDX_KEY, --hdx_key HDX_KEY
                        HDX api key
    -ua USER_AGENT, --user_agent USER_AGENT
                        user agent
    -hs HDX_SITE, --hdx_site HDX_SITE
                        HDX site to use
    -db DB_URI, --db_uri DB_URI
                        Database connection string
    -dp DB_PARAMS, --db_params DB_PARAMS
                        Database connection parameters. Overrides --db_uri.
    -gs GSHEET_AUTH, --gsheet_auth GSHEET_AUTH
                        Credentials for accessing Google Sheets
    -es EMAIL_SERVER, --email_server EMAIL_SERVER
                        Email server to use
    -fe FAILURE_EMAILS, --failure_emails FAILURE_EMAILS
                        People to alert on freshness failure
    -se SYSADMIN_EMAILS, --sysadmin_emails SYSADMIN_EMAILS
                        HDX system administrator emails
    -et EMAIL_TEST, --email_test EMAIL_TEST
                        Email only these test users for testing purposes
    -st, --spreadsheet_test
                        Use test instead of prod issues spreadsheet
    -ns, --no_spreadsheet
                        Do not update issues spreadsheet

## Cleaner

    python -m hdx.freshness.dbactions PARAMETERS

The PARAMETERS are:

    -db DB_URI, --db_uri DB_URI
                        Database connection string
    -dp DB_PARAMS, --db_params DB_PARAMS
                        Database connection parameters. Overrides --db_uri.
    -a ACTION, --action ACTION
                        Action to perform: `clone` or `clean` (the default).

