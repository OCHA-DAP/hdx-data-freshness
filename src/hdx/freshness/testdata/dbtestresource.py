# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String
from sqlalchemy import ForeignKey

from hdx.freshness.testdata.dbtestdataset import DBTestDataset


class DBTestResource(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    format = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBTestDataset.id), nullable=False)
    url = Column(String, nullable=False)
    metadata_modified = Column(String, nullable=False)
    last_modified = Column(String, nullable=False)

    def __repr__(self):
        output = '<Resource(id=%s, name=%s,\n' % (self.id, self.name)
        output += 'format=%s, dataset id=%s,\n' % (self.format, self.dataset_id)
        output += 'url=%s,\n' % self.url
        output += 'metadata modified=%s, ' % str(self.metadata_modified)
        output += 'last_modified=%s)>' % str(self.last_modified)
        return output
