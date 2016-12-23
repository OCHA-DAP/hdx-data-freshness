from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String
from sqlalchemy import ForeignKey

from database.base import Base
from database.dbdataset import DBDataset


class DBResource(Base):
    run_date = Column(DateTime, primary_key=True)
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBDataset.id), nullable=False)
    url = Column(String, nullable=False)
    error = Column(String)
    last_modified = Column(DateTime, nullable=False)
    revision_last_updated = Column(DateTime, default=None)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    what_updated = Column(String, nullable=False)

    def __repr__(self):
        output = '<Resource(run date=%s, id=%s, name=%s, ' % (str(self.run_date), self.id, self.name)
        output += 'url=%s, dataset id = %s,\n'%  (self.url, self.dataset_id)
        output += 'broken=%s, last_modified=%s, revision_last_updated=%s, ' % (self.broken_url, str(self.last_modified),
                                                                               str(self.revision_last_modified))
        output += 'http_last_modified=%s, MD5_hash=%s)>' % (str(self.http_last_modified), str(self.MD5_hash))
        output += 'what_updated=%s)>' % str(self.what_updated)
        return output
