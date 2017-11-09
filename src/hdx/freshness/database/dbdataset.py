# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from hdx.freshness.database.base import Base
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dbrun import DBRun


class DBDataset(Base):
    run_number = Column(Integer, ForeignKey(DBRun.run_number), primary_key=True)
    id = Column(String, ForeignKey(DBInfoDataset.id), primary_key=True)
    dataset_date = Column(String)
    update_frequency = Column(Integer)
    metadata_modified = Column(DateTime, nullable=False)
    last_modified = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    last_resource_updated = Column(String, nullable=False)
    last_resource_modified = Column(DateTime, nullable=False)
    fresh = Column(Integer)
    error = Column(Boolean, nullable=False)

    def __repr__(self):
        output = '<Dataset(run number=%d, id=%s, ' % (self.run_number, self.id)
        output += 'dataset date=%s, ' % str(self.dataset_date)
        output += 'update frequency=%s,\nlast_modified=%s' % (self.update_frequency, str(self.last_modified))
        output += 'what updated=%s, metadata_modified=%s,\n' % (str(self.what_updated), str(self.metadata_modified))
        output += 'Resource %s: last modified=%s,\n' % (str(self.last_resource_updated), str(self.last_resource_modified))
        output += 'Dataset fresh=%s' % str(self.fresh)
        return output
