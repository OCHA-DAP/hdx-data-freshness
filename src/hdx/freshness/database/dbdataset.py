"""SQLAlchemy class representing DBDataset row. Holds dynamic dataset metadata for
each run.
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


class DBDataset(Base):
    """
    run_number: Mapped[int] = mapped_column(
        ForeignKey("dbruns.run_number"), primary_key=True
    )
    id: Mapped[str] = mapped_column(
        ForeignKey("dbinfodatasets.id"), primary_key=True
    )
    dataset_date: Mapped[str] = mapped_column(nullable=True)
    update_frequency: Mapped[int] = mapped_column(nullable=True)
    review_date: Mapped[datetime] = mapped_column(nullable=True)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    updated_by_script: Mapped[datetime] = mapped_column(nullable=True)
    metadata_modified: Mapped[datetime] = mapped_column(
        nullable=False
    )  # this field and above are CKAN fields

    latest_of_modifieds: Mapped[datetime] = mapped_column(nullable=False)
    what_updated: Mapped[str] = mapped_column(nullable=False)
    last_resource_updated: Mapped[str] = mapped_column(
        nullable=False
    )  # id of last resource updated
    last_resource_modified: Mapped[datetime] = mapped_column(
        nullable=False
    )  # date last resource updated
    fresh: Mapped[int] = mapped_column(nullable=True)
    error: Mapped[bool] = mapped_column(nullable=False)
    """

    run_number: Mapped[int] = mapped_column(
        ForeignKey("dbruns.run_number"), primary_key=True
    )
    id: Mapped[str] = mapped_column(
        ForeignKey("dbinfodatasets.id"), primary_key=True
    )
    dataset_date: Mapped[str] = mapped_column(nullable=True)
    update_frequency: Mapped[int] = mapped_column(nullable=True)
    review_date: Mapped[datetime] = mapped_column(nullable=True)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    updated_by_script: Mapped[datetime] = mapped_column(nullable=True)
    metadata_modified: Mapped[datetime] = mapped_column(
        nullable=False
    )  # this field and above are CKAN fields
    latest_of_modifieds: Mapped[datetime] = mapped_column(nullable=False)
    what_updated: Mapped[str] = mapped_column(nullable=False)
    last_resource_updated: Mapped[str] = mapped_column(
        nullable=False
    )  # id of last resource updated
    last_resource_modified: Mapped[datetime] = mapped_column(
        nullable=False
    )  # date last resource updated
    fresh: Mapped[int] = mapped_column(nullable=True)
    error: Mapped[bool] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBDataset row

        Returns:
            str: String representation of DBDataset row
        """
        output = f"<Dataset(run number={self.run_number}, id={self.id}, "
        output += f"reference period={str(self.dataset_date)}, update frequency={self.update_frequency},\n"
        output += f"review date={str(self.review_date)}, last modified={str(self.last_modified)}, "
        output += f"metadata modified={str(self.metadata_modified)}, updated by script={str(self.updated_by_script)},\n"
        output += f"latest of modifieds={str(self.latest_of_modifieds)}, what updated={str(self.what_updated)},\n"
        output += f"Resource {str(self.last_resource_updated)}: last modified={str(self.last_resource_modified)},\n"
        output += f"Dataset fresh={str(self.fresh)}, error={str(self.error)})>"
        return output
