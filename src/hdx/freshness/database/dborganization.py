# -*- coding: utf-8 -*-
from sqlalchemy import Column, String

from hdx.freshness.database.base import Base


class DBOrganization(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)

    def __repr__(self):
        return '<Organization(id=%s, name=%s, title=%s)>' % (self.id, self.name, self.title)
