"""SQLAlchemy class representing DBOrganization row. Holds static organisation metadata.
"""
from hdx.database.no_timezone import Base
from sqlalchemy.orm import Mapped, mapped_column


class DBOrganization(Base):
    """
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=False)
    """

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBOrganization row

        Returns:
            str: String representation of DBOrganization row
        """
        return f"<Organization(id={self.id}, name={self.name}, title={self.title})>"
