"""SQLAlchemy class representing DBTestDataset row. Holds test data for datasets with
the aim of mimicking what would come from HDX.
"""
from sqlalchemy.orm import Mapped, mapped_column

from hdx.database.no_timezone import Base


class DBTestDataset(Base):
    """
    id: Mapped[str] = mapped_column(primary_key=True)
    organization_id: Mapped[str] = mapped_column(nullable=False)
    organization_name: Mapped[str] = mapped_column(nullable=False)
    organization_title: Mapped[str] = mapped_column(nullable=False)
    dataset_name: Mapped[str] = mapped_column(nullable=False)
    dataset_title: Mapped[str] = mapped_column(nullable=False)
    dataset_private: Mapped[bool] = mapped_column(nullable=False)
    dataset_maintainer: Mapped[str]
    dataset_date: Mapped[str]
    update_frequency: Mapped[str]
    review_date: Mapped[str]
    last_modified: Mapped[str] = mapped_column(nullable=False)
    updated_by_script: Mapped[str]
    metadata_modified: Mapped[str] = mapped_column(nullable=False)
    is_requestdata_type: Mapped[bool]
    dataset_location: Mapped[str]
    """

    id: Mapped[str] = mapped_column(primary_key=True)
    organization_id: Mapped[str] = mapped_column(nullable=False)
    organization_name: Mapped[str] = mapped_column(nullable=False)
    organization_title: Mapped[str] = mapped_column(nullable=False)
    dataset_name: Mapped[str] = mapped_column(nullable=False)
    dataset_title: Mapped[str] = mapped_column(nullable=False)
    dataset_private: Mapped[bool] = mapped_column(nullable=False)
    dataset_maintainer: Mapped[str] = mapped_column(nullable=False)
    dataset_date: Mapped[str] = mapped_column(nullable=False)
    update_frequency: Mapped[str] = mapped_column(nullable=False)
    review_date: Mapped[str] = mapped_column(nullable=True)
    last_modified: Mapped[str] = mapped_column(nullable=False)
    updated_by_script: Mapped[str] = mapped_column(nullable=True)
    metadata_modified: Mapped[str] = mapped_column(nullable=False)
    is_requestdata_type: Mapped[bool] = mapped_column(nullable=True)
    dataset_location: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        """String representation of DBTestDataset row

        Returns:
            str: String representation of DBTestDataset row
        """
        output = f"<Dataset(id={self.id}, name={self.dataset_name}"
        output += f"organisation name={self.organization_name},n"
        output += f"reference period={self.dataset_date}, update frequency={self.update_frequency}, "
        output += f"review_date={str(self.review_date)}, last_modified={str(self.last_modified)},"
        output += f"updated_by_script={str(self.updated_by_script)}, metadata_modified={str(self.metadata_modified)})>"
        return output
