# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr


class HDXBase(object):
    @declared_attr
    def __tablename__(cls):
        return '%ss' % cls.__name__.lower()

Base = declarative_base(cls=HDXBase)
