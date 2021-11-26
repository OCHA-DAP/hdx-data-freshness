"""SQLAlchemy class representing DBInfoDataset row. Holds static dataset metadata.
"""
from hdx.database import Base
from sqlalchemy import Boolean, Column, ForeignKey, String

from .dborganization import DBOrganization


class DBInfoDataset(Base):
    """
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    private = Column(Boolean, nullable=False)
    organization_id = Column(
        String, ForeignKey(DBOrganization.id), nullable=False
    )
    location = Column(String)
    maintainer = Column(String)
    """

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    private = Column(Boolean, nullable=False)
    organization_id = Column(
        String, ForeignKey(DBOrganization.id), nullable=False
    )
    location = Column(String)
    maintainer = Column(String)

    def __repr__(self) -> str:
        """String representation of DBInfoDataset row

        Returns:
            str: String representation of DBInfoDataset row
        """
        output = f"<InfoDataset(id={self.id}, name={self.name}, title={self.title},\n"
        output += f"private={str(self.private)}, organization id={self.organization_id},\n"
        output += f"maintainer={self.maintainer}, location={self.location})>"
        return output
