import logging

from ..database.dbdataset import DBDataset
from ..database.dbresource import DBResource
from ..database.dbrun import DBRun
from hdx.database import Database

logger = logging.getLogger(__name__)


class DBClone:
    # Clone database but limit to 1 dataset and its resources per run
    def __init__(self, session):
        self.session = session

    def run(
        self, params={"dialect": "sqlite", "database": "test_freshness.db"}
    ):
        with Database(**params) as clone_session:
            run_numbers = self.session.query(DBRun).all()
            for run_number in run_numbers:
                clone_session.merge(run_number)
                run_no = run_number.run_number
                logger.info(f"Adding run {run_no}")
                dbdataset = (
                    self.session.query(DBDataset)
                    .filter_by(run_number=run_no)
                    .first()
                )
                if not dbdataset:
                    logger.info(f"No datasets in run {run_no}")
                    continue
                clone_session.merge(dbdataset)
                dbresources = self.session.query(DBResource).filter_by(
                    run_number=run_no, dataset_id=dbdataset.id
                )
                for dbresource in dbresources:
                    clone_session.merge(dbresource)
                clone_session.commit()
