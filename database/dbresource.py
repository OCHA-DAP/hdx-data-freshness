from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from database.base import Base
from database.dbrun import DBRun


class DBResource(Base):
    run_number = Column(Integer, ForeignKey(DBRun.run_number), primary_key=True)
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey('dbdatasets.id'), nullable=False)
    url = Column(String, nullable=False)
    error = Column(String)
    last_modified = Column(DateTime, nullable=False)
    revision_last_updated = Column(DateTime, default=None)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    api=Column(Boolean)
    what_updated = Column(String, nullable=False)

    def __repr__(self):
        output = '<Resource(run number=%d, id=%s, name=%s, ' % (self.run_number, self.id, self.name)
        output += 'dataset id=%s,\nurl=%s,\n' % (self.dataset_id, self.url)
        output += 'error=%s, last_modified=%s, revision_last_updated=%s, ' % (self.error, str(self.last_modified),
                                                                              str(self.revision_last_updated))
        output += 'http_last_modified=%s, MD5_hash=%s, ' % (str(self.http_last_modified), str(self.md5_hash))
        output += 'api=%s, what_updated=%s)>' % (str(self.api), str(self.what_updated))
        return output
