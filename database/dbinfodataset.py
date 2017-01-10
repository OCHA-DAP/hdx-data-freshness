from sqlalchemy import Column, String
from sqlalchemy import ForeignKey

from database.base import Base


class DBInfoDataset(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    organization_id = Column(String, ForeignKey('dborganizations.id'), nullable=False)

    def __repr__(self):
        output = '<InfoDataset(id=%s, name=%s, title=%s\n' % (self.id, self.name, self.title)
        output += 'organization id=%s)>' % self.organization_id
        return output
