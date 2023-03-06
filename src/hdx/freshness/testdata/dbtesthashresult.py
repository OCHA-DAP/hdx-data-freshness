"""SQLAlchemy class representing DBTestResult row. Holds test data mimicking the result
of downloading and hashing urls (second time).
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy.orm import Mapped, mapped_column


class DBTestHashResult(Base):
    """
    id: Mapped[str] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(nullable=False)
    format: Mapped[str] = mapped_column(nullable=False)
    err: Mapped[str] = mapped_column(nullable=True)
    http_last_modified: Mapped[datetime] = mapped_column(nullable=True)
    hash: Mapped[str] = mapped_column(nullable=True)
    xlsx_hash: Mapped[str] = mapped_column(nullable=True)
    force_hash: Mapped[bool] = mapped_column(nullable=False)
    """

    id: Mapped[str] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(nullable=False)
    format: Mapped[str] = mapped_column(nullable=False)
    err: Mapped[str] = mapped_column(nullable=True)
    http_last_modified: Mapped[datetime] = mapped_column(nullable=True)
    hash: Mapped[str] = mapped_column(nullable=True)
    xlsx_hash: Mapped[str] = mapped_column(nullable=True)
    force_hash: Mapped[bool] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBTestHashresult row

        Returns:
            str: String representation of DBTestHashResult row
        """
        output = f"<TestHashResult(id={self.id}, url={self.url}, "
        output += f"format={self.format}, err={self.err}\n"
        output += f"http_last_modified={str(self.http_last_modified)}, "
        output += f"hash={self.hash}, xlsx_hash={self.xlsx_hash}, "
        output += f"force_hash={str(self.force_hash)})>"
        return output
