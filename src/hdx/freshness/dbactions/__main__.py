"""Entry point to start data freshness emailer
"""
import argparse
import logging
from os import getenv
from typing import Optional

from .. import __version__
from ..database import Base
from .dbclean import DBClean
from .dbclone import DBClone
from hdx.database import Database
from hdx.database.dburi import get_params_from_connection_uri
from hdx.utilities.dateparse import now_utc
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main(
    db_uri: Optional[str] = None,
    db_params: Optional[str] = None,
    action: str = "clean",
) -> None:
    """Run freshness database cleaner. Either a database connection string
    (db_uri) or database connection parameters (db_params) can be supplied. If
    neither is supplied, a local SQLite database with filename "freshness.db"
    is assumed.

    Args:
        db_uri (Optional[str]): Database connection URI. Defaults to None.
        db_params (Optional[str]): Database connection parameters. Defaults to None.
        action (bool): What action to take. "clone" to copy prod db for testing. Default is clean.

    Returns:
        None
    """

    logger.info(f"> Data freshness database clean {__version__}")
    if db_params:  # Get freshness database server details
        params = args_to_dict(db_params)
    elif db_uri:
        params = get_params_from_connection_uri(db_uri)
    else:
        params = {"dialect": "sqlite", "database": "freshness.db"}
    logger.info(f"> Database parameters: {params}")
    with Database(**params, table_base=Base) as session:
        now = now_utc()
        if action == "clean":
            cleaner = DBClean(session)
            cleaner.run(now)
            logger.info("Freshness database clean completed!")
        elif action == "clone":
            cloner = DBClone(session)
            cloner.run()
            logger.info("Freshness database clone completed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data Freshness Database Clean"
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
        "-a",
        "--action",
        default="clean",
        help="Action to perform.",
    )
    args = parser.parse_args()
    db_uri = args.db_uri
    if db_uri is None:
        db_uri = getenv("DB_URI")
    if db_uri and "://" not in db_uri:
        db_uri = f"postgresql://{db_uri}"
    main(
        db_uri=db_uri,
        db_params=args.db_params,
        action=args.action,
    )
