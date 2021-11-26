"""SQLAlchemy class representing DBDataset row. Holds dynamic dataset metadata for
each run.
"""
from hdx.database import Base
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String

from .dbinfodataset import DBInfoDataset
from .dbrun import DBRun


class DBDataset(Base):
    """
    run_number = Column(
        Integer, ForeignKey(DBRun.run_number), primary_key=True
    )

    id = Column(String, ForeignKey(DBInfoDataset.id), primary_key=True)

    dataset_date = Column(String)

    update_frequency = Column(Integer)

    review_date = Column(DateTime)

    last_modified = Column(DateTime, nullable=False)

    updated_by_script = Column(DateTime)

    metadata_modified = Column(
        DateTime, nullable=False
    )  # this field and above are CKAN fields

    latest_of_modifieds = Column(DateTime, nullable=False)

    what_updated = Column(String, nullable=False)

    last_resource_updated = Column(
        String, nullable=False
    )  # id of last resource updated

    last_resource_modified = Column(
        DateTime, nullable=False
    )  # date last resource updated

    fresh = Column(Integer)

    error = Column(Boolean, nullable=False)
    """

    run_number = Column(
        Integer, ForeignKey(DBRun.run_number), primary_key=True
    )
    id = Column(String, ForeignKey(DBInfoDataset.id), primary_key=True)
    dataset_date = Column(String)
    update_frequency = Column(Integer)
    review_date = Column(DateTime)
    last_modified = Column(DateTime, nullable=False)
    updated_by_script = Column(DateTime)
    metadata_modified = Column(
        DateTime, nullable=False
    )  # this field and above are CKAN fields
    latest_of_modifieds = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    last_resource_updated = Column(
        String, nullable=False
    )  # id of last resource updated
    last_resource_modified = Column(
        DateTime, nullable=False
    )  # date last resource updated
    fresh = Column(Integer)
    error = Column(Boolean, nullable=False)

    def __repr__(self) -> str:
        """String representation of DBDataset row

        Returns:
            str: String representation of DBDataset row
        """
        output = f"<Dataset(run number={self.run_number}, id={self.id}, "
        output += f"dataset date={str(self.dataset_date)}, update frequency={self.update_frequency},\n"
        output += f"review date={str(self.review_date)}, last modified={str(self.last_modified)}, "
        output += f"metadata modified={str(self.metadata_modified)}, updated by script={str(self.updated_by_script)},\n"
        output += f"latest of modifieds={str(self.latest_of_modifieds)}, what updated={str(self.what_updated)},\n"
        output += f"Resource {str(self.last_resource_updated)}: last modified={str(self.last_resource_modified)},\n"
        output += f"Dataset fresh={str(self.fresh)}, error={str(self.error)})>"
        return output
