# -*- coding: utf-8 -*-
from sqlalchemy import Boolean
from sqlalchemy import Column, String
from sqlalchemy import ForeignKey

from hdx.freshness.database.base import Base
from hdx.freshness.database.dborganization import DBOrganization


class DBInfoDataset(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    private = Column(Boolean, nullable=False)
    organization_id = Column(String, ForeignKey(DBOrganization.id), nullable=False)
    location = Column(String)
    maintainer = Column(String)
    maintainer_email = Column(String)
    author = Column(String)
    author_email = Column(String)

    def __repr__(self):
        output = '<InfoDataset(id=%s, name=%s, title=%s,\n' % (self.id, self.name, self.title)
        output += 'private=%s, organization id=%s,\n' % (str(self.private), self.organization_id)
        output += 'maintainer=%s, maintainer email=%s, ' % (self.maintainer, self.maintainer_email)
        output += 'author=%s, author email=%s, ' % (self.author, self.author_email)
        output += 'location=%s)>' % self.location
        return output
