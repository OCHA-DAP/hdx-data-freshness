# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime

from hdx.freshness.testdata.testbase import TestBase


class DBTestDate(TestBase):
    test_date = Column(DateTime, primary_key=True)

    def __repr__(self):
        return '<Test date=%s>' % str(self.test_date)
