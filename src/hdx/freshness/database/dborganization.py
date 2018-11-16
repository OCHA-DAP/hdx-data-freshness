# -*- coding: utf-8 -*-
from hdx.utilities.database import Base
from sqlalchemy import Column, String


class DBOrganization(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)

    def __repr__(self):
        return '<Organization(id=%s, name=%s, title=%s)>' % (self.id, self.name, self.title)
