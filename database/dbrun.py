from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import Integer

from database.base import Base


class DBRun(Base):
    run_date = Column(DateTime, primary_key=True)

    def __repr__(self):
        return '<Run date =%s>' % str(self.run_date)
