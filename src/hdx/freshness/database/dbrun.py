"""SQLAlchemy class representing DBRun row. Holds date of each run.
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy.orm import Mapped, mapped_column


class DBRun(Base):
    """
    run_number: Mapped[int] = mapped_column(primary_key=True)
    run_date: Mapped[datetime]
    """

    run_number: Mapped[int] = mapped_column(primary_key=True)
    run_date: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBRun row

        Returns:
            str: String representation of DBRun row
        """
        return f"<Run number={self.run_number}, Run date={str(self.run_date)}>"
