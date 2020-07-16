# -*- coding: utf-8 -*-
from hdx.utilities.database import Base
from sqlalchemy import Column, Boolean, String


class DBTestDataset(Base):
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

    def __repr__(self):
        output = '<Dataset(id=%s, name=%s' % (self.id, self.dataset_name)
        output += 'organisation name=%s,\n' % self.organization_name
        output += 'dataset date=%s, update frequency=%s, ' % (self.dataset_date, self.update_frequency)
        output += 'review_date=%s, last_modified=%s,' % (str(self.review_date), str(self.last_modified))
        output += 'updated_by_script=%s, metadata_modified=%s)>' % (
        str(self.updated_by_script), str(self.metadata_modified))
        return output
