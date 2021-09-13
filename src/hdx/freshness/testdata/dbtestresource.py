from hdx.database import Base
from sqlalchemy import Column, ForeignKey, String

from hdx.freshness.testdata.dbtestdataset import DBTestDataset


class DBTestResource(Base):
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    format = Column(String, nullable=False)
    dataset_id = Column(String, ForeignKey(DBTestDataset.id), nullable=False)
    url = Column(String, nullable=False)
    metadata_modified = Column(String, nullable=False)
    last_modified = Column(String, nullable=False)

    def __repr__(self):
        output = f"<Resource(id={self.id}, name={self.name},\n"
        output += f"format={self.format}, dataset id={self.dataset_id},\n"
        output += f"url={self.url},\n"
        output += f"metadata modified={str(self.metadata_modified)}, "
        output += f"last_modified={str(self.last_modified)})>"
        return output
