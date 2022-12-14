"""SQLAlchemy class representing DBRun row. Holds date of each run.
"""
from hdx.database import Base
from sqlalchemy import Column, Integer

from hdx.freshness.database import CustomDateTime


class DBRun(Base):
    """
    run_number = Column(Integer, primary_key=True)
    run_date = Column(CustomDateTime)
    """

    run_number = Column(Integer, primary_key=True)
    run_date = Column(CustomDateTime)

    def __repr__(self) -> str:
        """String representation of DBRun row

        Returns:
            str: String representation of DBRun row
        """
        return f"<Run number={self.run_number}, Run date={str(self.run_date)}>"
