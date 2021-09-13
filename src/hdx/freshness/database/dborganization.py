from hdx.database import Base
from sqlalchemy import Column, String


class DBOrganization(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)

    def __repr__(self):
        return f"<Organization(id={self.id}, name={self.name}, title={self.title})>"
