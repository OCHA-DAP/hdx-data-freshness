from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import Integer

from database.base import Base


class DBDataset(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_date = Column(String)
    update_frequency = Column(Integer)
    metadata_modified = Column(DateTime, nullable=False)
    last_modified = Column(DateTime, nullable=False)
    dataset_updated = Column(Integer, nullable=False)
    last_resource_updated = Column(String, nullable=False)
    fresh = Column(Integer)
    error = Column(Boolean, nullable=False)

    def __repr__(self):
        output = '<Dataset(id=%s, name=%s, dataset date=%s' % (self.id, self.name, str(self.dataset_date))
        output2 = 'update frequency=%s, metadata_modified=%s' % (self.update_frequency, str(self.metadata_modified))
        output3 = 'last_modified=%s, dataset updated=%s' % (str(self.last_modified), str(self.dataset_updated))
        output4 = 'last resource updated=%s, fresh=%s' % (str(self.last_resource_updated), str(self.fresh))
        return '%s\n%s%s' % (output, output2, output3, output4)
