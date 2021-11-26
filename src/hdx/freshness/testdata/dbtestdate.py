"""SQLAlchemy class representing DBTestDate row. Holds test data for dates.
"""
from hdx.database import Base
from sqlalchemy import Column, DateTime


class DBTestDate(Base):
    """
    test_date = Column(DateTime, primary_key=True)
    """

    test_date = Column(DateTime, primary_key=True)

    def __repr__(self) -> str:
        """String representation of DBTestDate row

        Returns:
            str: String representation of DBTestDate row
        """
        return f"<Test date={str(self.test_date)}>"
