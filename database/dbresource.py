from sqlalchemy import Boolean
from sqlalchemy import Column, DateTime, String

from database.base import Base


class DBResource(Base):
    id = Column(String, primary_key=True)
    name = Column(String)
    url = Column(String)
    broken_url = Column(Boolean, default=False)
    last_modified = Column(DateTime, default=None)
    revision_last_updated = Column(DateTime)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    last_hash_date = Column(DateTime, default=None)

    def __repr__(self):
        output = '<Resource(id="%s", name="%s", url="%s", broken="%s"' % (self.id, self.name, self.url, self.broken_url)
        output2 = 'last_modified="%s", revision_last_updated="%s", ' % (str(self.last_modified),
                                                                        str(self.revision_last_modified))
        output3 = 'http_last_modified="%s", MD5_hash="%s", hash_date="%s")>' % (str(self.http_last_modified),
                                                                                str(self.MD5_hash),
                                                                                str(self.last_hash_date))
        return '%s\n%s%s' % (output, output2, output3)