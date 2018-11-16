# -*- coding: utf-8 -*-
from hdx.utilities.database import Base
from sqlalchemy import Column, DateTime


class DBTestDate(Base):
    test_date = Column(DateTime, primary_key=True)

    def __repr__(self):
        return '<Test date=%s>' % str(self.test_date)
