from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import Integer

from database.base import Base


class DBDataset(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_date = Column(String)
    update_frequency = Column(Integer)
    last_modified = Column(DateTime, nullable=False)
    resource_updated = Column(String, nullable=False)
    error = Column(Boolean, nullable=False)

    def __repr__(self):
        output = '<Dataset(id=%s, name=%s, dataset date=%s' % (self.id, self.name, str(self.dataset_date))
        output2 = 'update frequency=%s, last_modified=%s' % (self.update_frequency, str(self.last_modified))
        output3 = 'resource updated=%s' % str(self.resource_updated)
        return '%s\n%s%s' % (output, output2, output3)
