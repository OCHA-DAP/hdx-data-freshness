"""SQLAlchemy class representing DBInfoDataset row. Holds static dataset metadata.
"""
from hdx.database.no_timezone import Base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


class DBInfoDataset(Base):
    """
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=False)
    private: Mapped[bool] = mapped_column(nullable=False)
    organization_id: Mapped[str] = mapped_column(
        ForeignKey("dborganizations.id"), nullable=False
    )
    location: Mapped[str] = mapped_column(nullable=True)
    maintainer: Mapped[str] = mapped_column(nullable=True)
    """

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    title: Mapped[str] = mapped_column(nullable=False)
    private: Mapped[bool] = mapped_column(nullable=False)
    organization_id: Mapped[str] = mapped_column(
        ForeignKey("dborganizations.id"), nullable=False
    )
    location: Mapped[str] = mapped_column(nullable=True)
    maintainer: Mapped[str] = mapped_column(nullable=True)

    def __repr__(self) -> str:
        """String representation of DBInfoDataset row

        Returns:
            str: String representation of DBInfoDataset row
        """
        output = f"<InfoDataset(id={self.id}, name={self.name}, title={self.title},\n"
        output += f"private={str(self.private)}, organization id={self.organization_id},\n"
        output += f"maintainer={self.maintainer}, location={self.location})>"
        return output
