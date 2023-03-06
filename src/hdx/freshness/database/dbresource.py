"""SQLAlchemy class representing DBResource row. Holds dynamic resource metadata for
each run.
"""
from datetime import datetime

from hdx.database.no_timezone import Base
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column


class DBResource(Base):
    """
    run_number: Mapped[int] = mapped_column(
        ForeignKey("dbruns.run_number"), primary_key=True
    )
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        ForeignKey("dbinfodatasets.id"), nullable=False
    )
    url: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    metadata_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )  # this field and above are CKAN fields

    latest_of_modifieds: Mapped[datetime] = mapped_column(nullable=False)
    what_updated: Mapped[str] = mapped_column(nullable=False)
    http_last_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )
    md5_hash: Mapped[str] = mapped_column(default=None, nullable=True)
    hash_last_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )
    when_checked: Mapped[datetime] = mapped_column(default=None, nullable=True)
    api: Mapped[bool] = mapped_column(nullable=True)
    error: Mapped[str] = mapped_column(nullable=True)
    """

    run_number: Mapped[int] = mapped_column(
        ForeignKey("dbruns.run_number"), primary_key=True
    )
    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    dataset_id: Mapped[str] = mapped_column(
        ForeignKey("dbinfodatasets.id"), nullable=False
    )
    url: Mapped[str] = mapped_column(nullable=False)
    last_modified: Mapped[datetime] = mapped_column(nullable=False)
    metadata_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )  # this field and above are CKAN fields
    latest_of_modifieds: Mapped[datetime] = mapped_column(nullable=False)
    what_updated: Mapped[str] = mapped_column(nullable=False)
    http_last_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )
    md5_hash: Mapped[str] = mapped_column(default=None, nullable=True)
    hash_last_modified: Mapped[datetime] = mapped_column(
        default=None, nullable=True
    )
    when_checked: Mapped[datetime] = mapped_column(default=None, nullable=True)
    api: Mapped[bool] = mapped_column(nullable=True)
    error: Mapped[str] = mapped_column(nullable=True)

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
