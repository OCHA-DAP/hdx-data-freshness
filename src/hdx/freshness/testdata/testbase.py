# -*- coding: utf-8 -*-
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr


class TestHDXBase(object):
    @declared_attr
    def __tablename__(cls):
        return '%ss' % cls.__name__.lower()


TestBase = declarative_base(cls=TestHDXBase)
