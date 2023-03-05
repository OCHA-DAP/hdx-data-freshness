"""SQLAlchemy class representing DBTestDate row. Holds test data for dates.
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy.orm import Mapped, mapped_column


class DBTestDate(Base):
    """
    test_date: Mapped[datetime] = mapped_column(primary_key=True)
    """

    test_date: Mapped[datetime] = mapped_column(primary_key=True)

    def __repr__(self) -> str:
        """String representation of DBTestDate row

        Returns:
            str: String representation of DBTestDate row
        """
        return f"<Test date={str(self.test_date)}>"
