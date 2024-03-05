"""Entry point to start data freshness emailer"""

import argparse
import logging
from os import getenv
from typing import Optional

from ... import __version__
from ...database import Base
from ..utils.databasequeries import DatabaseQueries
from ..utils.freshnessemail import Email
from ..utils.hdxhelper import HDXHelper
from ..utils.sheet import Sheet
from .datafreshnessstatus import DataFreshnessStatus
from hdx.api.configuration import Configuration
from hdx.database import Database
from hdx.database.dburi import get_params_from_connection_uri
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file

setup_logging()
logger = logging.getLogger(__name__)


def main(
    db_uri: Optional[str] = None,
    db_params: Optional[str] = None,
    gsheet_auth: Optional[str] = None,
    email_server: Optional[str] = None,
    failure_emails: Optional[str] = None,
    sysadmin_emails: Optional[str] = None,
    email_test: Optional[str] = None,
    spreadsheet_test: bool = False,
    no_spreadsheet: bool = False,
    **ignore,
) -> None:
    """Run freshness emailer. Either a database connection string (db_uri) or database
    connection parameters (db_params) can be supplied. If neither is supplied, a local
    SQLite database with filename "freshness.db" is assumed. An optional email server
    can be supplied in the form:
    connection type (eg. ssl),host,port,username,password,sender email

    If not supplied, no emails will be sent. An optional authorisation string for
    Google Sheets can be supplied of the form:
    {"type": "service_account", "project_id": "hdx-bot", "private_key_id": ...
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",...}

    failure_emails is a list of email addresses for the people who should be emailed in
    the event of a freshness failure. sysadmin_emails is a list of email addresses of
    HDX system administrators who are emailed with summaries of maintainers contacted,
    datasets that have become delinquent, invalid maintainers and org admins etc.

    Args:
        db_uri (Optional[str]): Database connection URI. Defaults to None.
        db_params (Optional[str]): Database connection parameters. Defaults to None.
        gsheet_auth (Optional[str]): Google Sheets authorisation. Defaults to None.
        email_server (Optional[str]): Email server to use. Defaults to None.
        failure_emails (Optional[str]): Email addresses. Defaults to None.
        sysadmin_emails (Optional[str]): Email addresses. Defaults to None.
        email_test (Optional[str]): Only email test users. Defaults to None.
        spreadsheet_test (bool): Output to test Google spreadsheet. Defaults to False.
        no_spreadsheet (bool): Don't output to Google spreadsheet. Defaults to False.

    Returns:
        None
    """

    logger.info(f"> Data freshness emailer {__version__}")
    configuration = Configuration.read()
    if email_server:  # Get email server details
        email_config = email_server.split(",")
        email_config_dict = {
            "connection_type": email_config[0],
            "host": email_config[1],
            "port": int(email_config[2]),
            "username": email_config[3],
            "password": email_config[4],
        }
        if len(email_config) > 5:
            email_config_dict["sender"] = email_config[5]
        configuration.setup_emailer(email_config_dict=email_config_dict)
        logger.info(f"> Email host: {email_config[1]}")
        send_emails = configuration.emailer().send
    else:
        logger.info("> No email host!")
        send_emails = None
    if db_params:  # Get freshness database server details
        params = args_to_dict(db_params)
    elif db_uri:
        params = get_params_from_connection_uri(db_uri)
    else:
        params = {"dialect": "sqlite", "database": "freshness.db"}
    if sysadmin_emails:
        sysadmin_emails = sysadmin_emails.split(",")
    logger.info(f"> Database parameters: {params}")
    with Database(**params, table_base=Base) as session:
        now = now_utc()
        email = Email(
            now,
            sysadmin_emails=sysadmin_emails,
            send_emails=send_emails,
        )
        sheet = Sheet(now)

        if failure_emails:
            failure_emails = failure_emails.split(",")
        else:
            failure_emails = list()
        error = sheet.setup_gsheet(
            configuration, gsheet_auth, spreadsheet_test, no_spreadsheet
        )
        if error:
            email.htmlify_send(
                failure_emails, "Error opening Google sheets!", error
            )
        else:
            error = sheet.setup_input()
            if error:
                email.htmlify_send(
                    failure_emails,
                    "Error reading DP duty roster or data grid curation sheet!",
                    error,
                )
            else:
                hdxhelper = HDXHelper(
                    site_url=configuration.get_hdx_site_url()
                )
                databasequeries = DatabaseQueries(
                    session=session, now=now, hdxhelper=hdxhelper
                )
                freshness = DataFreshnessStatus(
                    databasequeries=databasequeries,
                    email=email,
                    sheet=sheet,
                )
                # Check number of datasets hasn't dropped
                if not freshness.check_number_datasets(
                    now, send_failures=failure_emails
                ):
                    if email_test:  # send just to test users
                        test_users = [failure_emails[0]]
                        freshness.process_broken(recipients=test_users)
                        freshness.process_overdue(
                            recipients=test_users, sysadmins=test_users
                        )
                        freshness.process_delinquent(recipients=test_users)
                        freshness.process_maintainer_orgadmins(
                            recipients=test_users
                        )
                        freshness.process_datasets_noresources(
                            recipients=test_users
                        )
                        # freshness.process_datasets_time_period(
                        #     recipients=test_users,
                        #     sysadmins=test_users
                        # )
                        freshness.process_datasets_datagrid(
                            recipients=test_users
                        )
                    else:
                        # freshness.process_broken()  # Check for broken resources
                        freshness.process_overdue()  # Check for overdue datasets
                        # freshness.process_delinquent()  # Check for delinquent datasets
                        # Check for datasets with invalid maintainer and organisations
                        # with invalid administrators
                        freshness.process_maintainer_orgadmins()
                        # Check for datasets with no resources
                        freshness.process_datasets_noresources()
                        # Check for datasets where the time period may need updating
                        # freshness.process_datasets_time_period(
                        #     sysadmins=test_users
                        # )
                        # Check for candidates for the data grid
                        freshness.process_datasets_datagrid()

    logger.info("Freshness emailer completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Freshness Emailer")
    parser.add_argument("-hk", "--hdx_key", default=None, help="HDX api key")
    parser.add_argument("-ua", "--user_agent", default=None, help="user agent")
    parser.add_argument("-pp", "--preprefix", default=None, help="preprefix")
    parser.add_argument(
        "-hs", "--hdx_site", default=None, help="HDX site to use"
    )
    parser.add_argument(
        "-db", "--db_uri", default=None, help="Database connection string"
    )
    parser.add_argument(
        "-dp",
        "--db_params",
        default=None,
        help="Database connection parameters. Overrides --db_uri.",
    )
    parser.add_argument(
        "-gs",
        "--gsheet_auth",
        default=None,
        help="Credentials for accessing Google Sheets",
    )
    parser.add_argument(
        "-es", "--email_server", default=None, help="Email server to use"
    )
    parser.add_argument(
        "-fe",
        "--failure_emails",
        default=None,
        help="People to alert on freshness failure",
    )
    parser.add_argument(
        "-se",
        "--sysadmin_emails",
        default=None,
        help="HDX system administrator emails",
    )
    parser.add_argument(
        "-et",
        "--email_test",
        default=None,
        help="Email only these test users for testing purposes",
    )
    parser.add_argument(
        "-st",
        "--spreadsheet_test",
        default=False,
        action="store_true",
        help="Use test instead of prod issues spreadsheet",
    )
    parser.add_argument(
        "-ns",
        "--no_spreadsheet",
        default=False,
        action="store_true",
        help="Do not update issues spreadsheet",
    )
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
        if user_agent is None:
            user_agent = "freshness-emailer"
    preprefix = args.preprefix
    if preprefix is None:
        preprefix = getenv("PREPREFIX")
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = getenv("HDX_SITE", "prod")
    db_uri = args.db_uri
    if db_uri is None:
        db_uri = getenv("DB_URI")
    if db_uri and "://" not in db_uri:
        db_uri = f"postgresql://{db_uri}"
    gsheet_auth = args.gsheet_auth
    if gsheet_auth is None:
        gsheet_auth = getenv("GSHEET_AUTH")
    email_server = args.email_server
    if email_server is None:
        email_server = getenv("EMAIL_SERVER")
    failure_emails = args.failure_emails
    if failure_emails is None:
        failure_emails = getenv("FAILURE_EMAILS")
    sysadmin_emails = args.sysadmin_emails
    if sysadmin_emails is None:
        sysadmin_emails = getenv("SYSADMIN_EMAILS")
    project_config_yaml = script_dir_plus_file(
        "project_configuration.yaml", main
    )
    facade(
        main,
        hdx_key=hdx_key,
        user_agent=user_agent,
        preprefix=preprefix,
        hdx_site=hdx_site,
        project_config_yaml=project_config_yaml,
        db_uri=db_uri,
        db_params=args.db_params,
        gsheet_auth=gsheet_auth,
        email_server=email_server,
        failure_emails=failure_emails,
        sysadmin_emails=sysadmin_emails,
        email_test=args.email_test,
        spreadsheet_test=args.spreadsheet_test,
        no_spreadsheet=args.no_spreadsheet,
    )
