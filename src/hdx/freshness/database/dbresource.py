# -*- coding: utf-8 -*-
from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from hdx.freshness.database.base import Base
from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dbrun import DBRun


class DBResource(Base):
    run_number = Column(Integer, ForeignKey(DBRun.run_number), primary_key=True)
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBInfoDataset.id), nullable=False)
    url = Column(String, nullable=False)
    error = Column(String)
    last_modified = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    revision_last_updated = Column(DateTime, default=None)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    when_hashed = Column(DateTime, default=None)
    when_checked = Column(DateTime, default=None)
    api = Column(Boolean)

    def __repr__(self):
        output = '<Resource(run number=%d, id=%s, name=%s, ' % (self.run_number, self.id, self.name)
        output += 'dataset id=%s,\nurl=%s,\n' % (self.dataset_id, self.url)
        output += 'error=%s, last modified=%s, what updated=%s,\n' % (self.error, str(self.last_modified),
                                                                     str(self.what_updated))
        output += 'revision last updated=%s, http last modified=%s, ' % (str(self.revision_last_updated),
                                                                         str(self.http_last_modified))
        output += 'MD5 hash=%s, when hashed=%s, ' % (str(self.http_last_modified), str(self.when_hashed))
        output += 'when checked=%s, ' % str(self.when_checked)
        output += 'api=%s)>' % str(self.api)
        return output
