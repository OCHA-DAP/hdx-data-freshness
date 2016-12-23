from sqlalchemy import Column, DateTime, Boolean, String
from sqlalchemy import Integer

from database.base import Base


class DBDataset(Base):
    run_date = Column(DateTime, primary_key=True)
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
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
        output = '<Dataset(run date =%s, id=%s, name=%s, ' % (str(self.run_date), self.id, self.name)
        output += 'dataset date=%s, ' % str(self.dataset_date)
        output += 'update frequency=%s,\nlast_modified=%s' % (self.update_frequency, str(self.last_modified))
        output += 'what updated=%s, metadata_modified=%s,\n' % (str(self.what_updated), str(self.metadata_modified))
        output += 'Resource %s: last modified=%s,\n' % (str(self.last_resource_updated), str(self.last_resource_modified))
        output += 'Dataset fresh=%s' % str(self.fresh)
        return output
