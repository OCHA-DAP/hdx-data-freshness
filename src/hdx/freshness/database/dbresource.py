# -*- coding: utf-8 -*-
from hdx.utilities.database import Base
from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from hdx.freshness.database.dbinfodataset import DBInfoDataset
from hdx.freshness.database.dbrun import DBRun


class DBResource(Base):
    run_number = Column(Integer, ForeignKey(DBRun.run_number), primary_key=True)
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBInfoDataset.id), nullable=False)
    url = Column(String, nullable=False)
    last_modified = Column(DateTime, nullable=False)
    revision_last_updated = Column(DateTime, default=None)  # this field and above are CKAN fields
    latest_of_modifieds = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    when_hashed = Column(DateTime, default=None)
    when_checked = Column(DateTime, default=None)
    api = Column(Boolean)
    error = Column(String)

    def __repr__(self):
        output = '<Resource(run number=%d, id=%s, name=%s, ' % (self.run_number, self.id, self.name)
        output += 'dataset id=%s,\nurl=%s,\n' % (self.dataset_id, self.url)
        output += 'last modified=%s, revision last updated=%s,\n' % (str(self.last_modified),
                                                                     str(self.revision_last_updated))
        output += 'latest of modifieds=%s, what updated=%s,\n' % (str(self.latest_of_modifieds), str(self.what_updated))
        output += 'http last modified=%s,\n' % str(self.http_last_modified)
        output += 'MD5 hash=%s, when hashed=%s, ' % (str(self.http_last_modified), str(self.when_hashed))
        output += 'when checked=%s,\n' % str(self.when_checked)
        output += 'api=%s, error=%s)>' % (str(self.api), str(self.error))
        return output
