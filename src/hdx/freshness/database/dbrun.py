"""SQLAlchemy class representing DBRun row. Holds date of each run.
"""
from hdx.database import Base
from sqlalchemy import Column, DateTime, Integer


class DBRun(Base):
    """
    run_number = Column(Integer, primary_key=True)
    run_date = Column(DateTime)
    """

    run_number = Column(Integer, primary_key=True)
    run_date = Column(DateTime)

    def __repr__(self) -> str:
        """String representation of DBRun row

        Returns:
            str: String representation of DBRun row
        """
        return f"<Run number={self.run_number}, Run date={str(self.run_date)}>"
