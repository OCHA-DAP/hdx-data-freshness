from hdx.database import Base
from sqlalchemy import Boolean, Column, DateTime, String


class DBTestResult(Base):
    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    format = Column(String, nullable=False)
    err = Column(String)
    http_last_modified = Column(DateTime)
    hash = Column(String)
    force_hash = Column(Boolean, nullable=False)

    def __repr__(self):
        output = f"<TestResult(id={self.id}, url={self.url}, "
        output += f"format={self.format}, err={self.err}\n"
        output += (
            f"http_last_modified={str(self.http_last_modified)}, hash={self.hash},"
        )
        output += f"force_hash={str(self.force_hash)})>"
        return output
