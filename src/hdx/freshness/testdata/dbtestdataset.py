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
    dataset_maintainer_email = Column(String)
    dataset_author = Column(String)
    dataset_author_email = Column(String)
    dataset_date = Column(String)
    metadata_modified = Column(String, nullable=False)
    review_date = Column(String)
    last_modified = Column(String, nullable=False)
    update_frequency = Column(String)
    is_requestdata_type = Column(Boolean)
    dataset_location = Column(String)

    def __repr__(self):
        output = '<Dataset(id=%s, name=%s' % (self.id, self.dataset_name)
        output += 'organisation name=%s, dataset date=%s, ' % (self.organization_name, self.dataset_date)
        output += 'update frequency=%s, metadata_modified=%s, ' % (self.update_frequency, str(self.metadata_modified))
        output += 'review_date=%s, last_modified=%s)>' % (str(self.review_date), str(self.last_modified))
        return output
