# -*- coding: utf-8 -*-
from sqlalchemy import Column, String
from sqlalchemy import ForeignKey

from hdx.freshness.testdata.dbtestdataset import DBTestDataset
from hdx.freshness.testdata.testbase import TestBase


class DBTestResource(TestBase):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    format = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBTestDataset.id), nullable=False)
    url = Column(String, nullable=False)
    revision_last_updated = Column(String, nullable=False)

    def __repr__(self):
        output = '<Resource(run number=%d, id=%s,\n' % (self.run_number, self.id)
        output += 'format=%s, dataset id=%s,\n' % (self.format, self.dataset_id)
        output += 'url=%s,\n' % self.url
        output += 'revision last updated=%s)>' % str(self.revision_last_updated)
        return output
