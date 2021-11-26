"""SQLAlchemy class representing DBResource row. Holds dynamic resource metadata for
each run.
"""
from hdx.database import Base
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String

from .dbinfodataset import DBInfoDataset
from .dbrun import DBRun


class DBResource(Base):
    """
    run_number = Column(
        Integer, ForeignKey(DBRun.run_number), primary_key=True
    )
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBInfoDataset.id), nullable=False)
    url = Column(String, nullable=False)
    last_modified = Column(DateTime, nullable=False)
    metadata_modified = Column(
        DateTime, default=None
    )  # this field and above are CKAN fields
    latest_of_modifieds = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    hash_last_modified = Column(DateTime, default=None)
    when_checked = Column(DateTime, default=None)
    api = Column(Boolean)
    error = Column(String)
    """

    run_number = Column(
        Integer, ForeignKey(DBRun.run_number), primary_key=True
    )
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBInfoDataset.id), nullable=False)
    url = Column(String, nullable=False)
    last_modified = Column(DateTime, nullable=False)
    metadata_modified = Column(
        DateTime, default=None
    )  # this field and above are CKAN fields
    latest_of_modifieds = Column(DateTime, nullable=False)
    what_updated = Column(String, nullable=False)
    http_last_modified = Column(DateTime, default=None)
    md5_hash = Column(String, default=None)
    hash_last_modified = Column(DateTime, default=None)
    when_checked = Column(DateTime, default=None)
    api = Column(Boolean)
    error = Column(String)

    def __repr__(self) -> str:
        """String representation of DBResource row

        Returns:
            str: String representation of DBResource row
        """
        output = f"<Resource(run number={self.run_number}, id={self.id}, name={self.name}, "
        output += f"dataset id={self.dataset_id},\nurl={self.url},\n"
        output += f"last modified={str(self.last_modified)}, metadata modified={str(self.metadata_modified)},\n"
        output += f"latest of modifieds={str(self.latest_of_modifieds)}, what updated={str(self.what_updated)},\n"
        output += f"http last modified={str(self.http_last_modified)},\n"
        output += f"MD5 hash={self.md5_hash}, hash last modified={str(self.hash_last_modified)}, "
        output += f"when checked={str(self.when_checked)},\n"
        output += f"api={str(self.api)}, error={str(self.error)})>"
        return output
