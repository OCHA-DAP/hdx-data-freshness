from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String
from sqlalchemy import ForeignKey

from database.base import Base
from database.dbdataset import DBDataset


class DBResource(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBDataset.id), nullable=False)
    url = Column(String, nullable=False)
    error = Column(String)
    last_modified = Column(DateTime, nullable=False)
    revision_last_updated = Column(DateTime, default=None)
    http_last_modified = Column(DateTime, default=None)
    last_hash_date = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    updated = Column(String, nullable=False)

    def __repr__(self):
        output = '<Resource(id=%s, name=%s, url=%s, dataset id = %s,\n' % (self.id, self.name, self.url, self.dataset_id)
        output += 'broken=%s, last_modified=%s, revision_last_updated=%s, ' % (self.broken_url, str(self.last_modified),
                                                                               str(self.revision_last_modified))
        output += 'http_last_modified=%s, MD5_hash=%s, hash_date=%s)>' % (str(self.http_last_modified),
                                                                          str(self.MD5_hash),
                                                                          str(self.last_hash_date))
        output += 'updated=%s)>' % str(self.updated)
        return output
