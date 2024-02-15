from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, declared_attr

from hdx.database.no_timezone import ConversionNoTZ


class Base(DeclarativeBase):
    type_annotation_map = {
        datetime: ConversionNoTZ,
    }

    @declared_attr.directive
    def __tablename__(cls):
        return f"{cls.__name__.lower()}s"
