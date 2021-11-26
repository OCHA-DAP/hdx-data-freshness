"""SQLAlchemy class representing DBTestDataset row. Holds test data for datasets with
the aim of mimicking what would come from HDX.
"""
from hdx.database import Base
from sqlalchemy import Boolean, Column, String


class DBTestDataset(Base):
    """
    id = Column(String, primary_key=True)\
    organization_id = Column(String, nullable=False)\
    organization_name = Column(String, nullable=False)\
    organization_title = Column(String, nullable=False)\
    dataset_name = Column(String, nullable=False)\
    dataset_title = Column(String, nullable=False)\
    dataset_private = Column(Boolean, nullable=False)\
    dataset_maintainer = Column(String)\
    dataset_date = Column(String)\
    update_frequency = Column(String)\
    review_date = Column(String)\
    last_modified = Column(String, nullable=False)\
    updated_by_script = Column(String)\
    metadata_modified = Column(String, nullable=False)\
    is_requestdata_type = Column(Boolean)\
    dataset_location = Column(String)
    """

    id = Column(String, primary_key=True)
    organization_id = Column(String, nullable=False)
    organization_name = Column(String, nullable=False)
    organization_title = Column(String, nullable=False)
    dataset_name = Column(String, nullable=False)
    dataset_title = Column(String, nullable=False)
    dataset_private = Column(Boolean, nullable=False)
    dataset_maintainer = Column(String)
    dataset_date = Column(String)
    update_frequency = Column(String)
    review_date = Column(String)
    last_modified = Column(String, nullable=False)
    updated_by_script = Column(String)
    metadata_modified = Column(String, nullable=False)
    is_requestdata_type = Column(Boolean)
    dataset_location = Column(String)

    def __repr__(self) -> str:
        """String representation of DBTestDataset row

        Returns:
            str: String representation of DBTestDataset row
        """
        output = f"<Dataset(id={self.id}, name={self.dataset_name}"
        output += f"organisation name={self.organization_name},\n"
        output += f"dataset date={self.dataset_date}, update frequency={self.update_frequency}, "
        output += f"review_date={str(self.review_date)}, last_modified={str(self.last_modified)},"
        output += f"updated_by_script={str(self.updated_by_script)}, metadata_modified={str(self.metadata_modified)})>"
        return output
