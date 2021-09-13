from hdx.database import Base
from sqlalchemy import Boolean, Column, ForeignKey, String

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
        output = f"<InfoDataset(id={self.id}, name={self.name}, title={self.title},\n"
        output += (
            f"private={str(self.private)}, organization id={self.organization_id},\n"
        )
        output += f"maintainer={self.maintainer}, location={self.location})>"
        return output
