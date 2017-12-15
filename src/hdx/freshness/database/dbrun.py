# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime
from sqlalchemy import Integer

from hdx.freshness.database.base import Base


class DBRun(Base):
    run_number = Column(Integer, primary_key=True)
    run_date = Column(DateTime)

    def __repr__(self):
        return '<Run number=%d, Run date=%s>' % (self.run_number, str(self.run_date))
