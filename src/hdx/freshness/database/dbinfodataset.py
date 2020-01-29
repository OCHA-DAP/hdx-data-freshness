# -*- coding: utf-8 -*-
from hdx.utilities.database import Base
from sqlalchemy import Boolean
from sqlalchemy import Column, String
from sqlalchemy import ForeignKey

from hdx.freshness.database.dborganization import DBOrganization


class DBInfoDataset(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    private = Column(Boolean, nullable=False)
    organization_id = Column(String, ForeignKey(DBOrganization.id), nullable=False)
    location = Column(String)
    maintainer = Column(String)

    def __repr__(self):
        output = '<InfoDataset(id=%s, name=%s, title=%s,\n' % (self.id, self.name, self.title)
        output += 'private=%s, organization id=%s,\n' % (str(self.private), self.organization_id)
        output += 'maintainer=%s, location=%s)>' % (self.maintainer, self.location)
        return output
