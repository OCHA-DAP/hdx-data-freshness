from hdx.database import Base
from sqlalchemy import Column, DateTime, Integer


class DBRun(Base):
    run_number = Column(Integer, primary_key=True)
    run_date = Column(DateTime)

    def __repr__(self):
        return f"<Run number={self.run_number}, Run date={str(self.run_date)}>"
