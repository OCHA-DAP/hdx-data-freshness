"""SQLAlchemy class representing DBTestResult row. Holds test data mimicking the result
of downloading and hashing urls (second time).
"""
from hdx.database import Base
from sqlalchemy import Boolean, Column, DateTime, String


class DBTestHashResult(Base):
    """
    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    format = Column(String, nullable=False)
    err = Column(String)
    http_last_modified = Column(DateTime)
    hash = Column(String)
    xlsx_hash = Column(String)
    force_hash = Column(Boolean, nullable=False)
    """

    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    format = Column(String, nullable=False)
    err = Column(String)
    http_last_modified = Column(DateTime)
    hash = Column(String)
    xlsx_hash = Column(String)
    force_hash = Column(Boolean, nullable=False)

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
