"""SQLAlchemy class representing DBTestResource row. Holds test data for resources with
the aim of mimicking what would come from HDX.
"""
from hdx.database.no_timezone import Base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


class DBTestResource(Base):
    """
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    format: Mapped[str] = mapped_column(nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        ForeignKey("dbtestdatasets.id"), nullable=False
    )
    url: Mapped[str] = mapped_column(nullable=False)
    metadata_modified: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[str] = mapped_column(nullable=False)
    """

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    format: Mapped[str] = mapped_column(nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        ForeignKey("dbtestdatasets.id"), nullable=False
    )
    url: Mapped[str] = mapped_column(nullable=False)
    metadata_modified: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBTestResource row

        Returns:
            str: String representation of DBTestResource row
        """
        output = f"<Resource(id={self.id}, name={self.name},\n"
        output += f"format={self.format}, dataset id={self.dataset_id},\n"
        output += f"url={self.url},\n"
        output += f"metadata modified={str(self.metadata_modified)}, "
        output += f"last_modified={str(self.last_modified)})>"
        return output
