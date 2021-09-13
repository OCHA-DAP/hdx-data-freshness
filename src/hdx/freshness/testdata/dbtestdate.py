from hdx.database import Base
from sqlalchemy import Column, DateTime


class DBTestDate(Base):
    test_date = Column(DateTime, primary_key=True)

    def __repr__(self):
        return f"<Test date={str(self.test_date)}>"
