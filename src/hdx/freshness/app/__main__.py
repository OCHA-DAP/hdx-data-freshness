"""Entry point to start data freshness
"""
import argparse
import logging
from os import getenv
from typing import Optional

from hdx.database import Database
from hdx.database.dburi import get_params_from_connection_uri
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

from hdx.freshness.app import __version__
from hdx.freshness.app.datafreshness import DataFreshness

setup_logging()
logger = logging.getLogger(__name__)


def main(
    db_uri: Optional[str] = None,
    db_params: Optional[str] = None,
    do_touch: bool = True,
    save: bool = False,
    **ignore,
) -> None:
    """Run freshness. Either a database connection string (db_uri) or database
    connection parameters (db_params) can be supplied. If neither is supplied, a local
    SQLite database with filename "freshness.db" is assumed.

    Args:
        db_uri (Optional[str]): Database connection URI. Defaults to None.
        db_params (Optional[str]): Database connection parameters. Defaults to None.
        do_touch (bool): Touch HDX datasets if files change. Defaults to False.
        save (bool): Whether to save state for testing. Defaults to False.

    Returns:
        None
    """
    logger.info(f"> Data freshness {__version__}")
    if db_params:
        params = args_to_dict(db_params)
    elif db_uri:
        params = get_params_from_connection_uri(db_uri)
    else:
        params = {"dialect": "sqlite", "database": "freshness.db"}
    logger.info(f"> Database parameters: {params}")
    with Database(**params) as session:
        testsession = None
        if save:
            testsession = Database.get_session("sqlite:///test_serialize.db")
        # Setup including reading all datasets from HDX and setting threshold for how
        # many resources to force hash
        freshness = DataFreshness(
            session=session, testsession=testsession, do_touch=do_touch
        )
        # Arrange order of list of datasets so that datasets from the same organisation
        # are moved away from each other
        freshness.spread_datasets()
        # Add new run number and date
        freshness.add_new_run()
        # Store metadata in freshness database, calculate an initial freshness based on
        # the metadata and determine resources to be downloaded and hashed
        datasets_to_check, resources_to_check = freshness.process_datasets()
        # Download resource urls and hash them
        results, hash_results = freshness.check_urls(
            resources_to_check, UserAgent.get()
        )
        # Work out if hashed files have changed and if so, touch resources.
        # Determine latest of modifieds and freshness for all datasets
        datasets_resourcesinfo = freshness.process_results(
            results, hash_results
        )
        # Work out latest_of_modifieds for datasets and calculate freshness
        freshness.update_dataset_latest_of_modifieds(
            datasets_to_check, datasets_resourcesinfo
        )
        # Display output string
        freshness.output_counts()
        if testsession:
            testsession.close()
    logger.info("Freshness completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Freshness")
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
        "-dt",
        "--donttouch",
        default=False,
        action="store_true",
        help="Don't touch datasets",
    )
    parser.add_argument(
        "-s",
        "--save",
        default=False,
        action="store_true",
        help="Save state for testing",
    )
    args = parser.parse_args()
    hdx_key = args.hdx_key
    if hdx_key is None:
        hdx_key = getenv("HDX_KEY")
    user_agent = args.user_agent
    if user_agent is None:
        user_agent = getenv("USER_AGENT")
        if user_agent is None:
            user_agent = "freshness"
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
    project_config_yaml = script_dir_plus_file(
        "project_configuration.yml", main
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
        do_touch=not args.donttouch,
        save=args.save,
    )
