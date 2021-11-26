"""SQLAlchemy class representing DBOrganization row. Holds static organisation metadata.
"""
from hdx.database import Base
from sqlalchemy import Column, String


class DBOrganization(Base):
    """
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    """

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    title = Column(String, nullable=False)

    def __repr__(self) -> str:
        """String representation of DBOrganization row

        Returns:
            str: String representation of DBOrganization row
        """
        return f"<Organization(id={self.id}, name={self.name}, title={self.title})>"
