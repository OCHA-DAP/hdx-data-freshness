# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime, Boolean, String

from hdx.freshness.testdata.testbase import TestBase


class DBTestResult(TestBase):
    id = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    err = Column(String)
    http_last_modified = Column(DateTime)
    hash = Column(String)
    force_hash = Column(Boolean, nullable=False)

    def __repr__(self):
        output = '<TestResult(id=%s, url=%s, ' % (self.id, self.url)
        output += 'err=%s\n' % self.err
        output += 'http_last_modified=%s, hash=%s,' % (str(self.http_last_modified), self.hash)
        output += 'force_hash=%s)>' % str(self.force_hash)
        return output
