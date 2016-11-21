from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import declared_attr


class HDXBase(object):
    @declared_attr
    def __tablename__(cls):
        return '%ss' % cls.__name__.lower()

    id = Column(Integer, primary_key=True)

Base = declarative_base(cls=HDXBase)
