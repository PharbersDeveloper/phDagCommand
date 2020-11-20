import uuid
from datetime import datetime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, Float


Base = declarative_base()


class DataSet(Base):
    __tablename__ = 'dataSet'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    parent = Column(String, default='{}')
    child = Column(String, default='{}')
    blockDs = Column(String)
    job = Column(String)
    sampleData = Column(String, default='{}')
    name = Column(String)
    schema = Column(String, default='{}')
    source = Column(String)
    storeType = Column(String, default='parquet')
    size = Column(Float)
    created = Column(DateTime, default=datetime.now())
    modified = Column(DateTime, default=datetime.now())
    description = Column(String)
    url = Column(String)
    tabName = Column(String)
    status = Column(String)
    mart = Column(String)
    assetDs = Column(String)
    colNames = Column(String, default='{}')
    length = Column(Float)

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        self.parent = kwargs.get('parent', None)
        self.child = kwargs.get('child', None)
        self.blockDs = kwargs.get('blockDs', None)
        self.job = kwargs.get('job', None)
        self.sampleData = kwargs.get('sampleData', None)
        self.name = kwargs.get('name', None)
        self.schema = kwargs.get('schema', None)
        self.source = kwargs.get('source', None)
        self.storeType = kwargs.get('storeType', None)
        self.size = kwargs.get('size', None)
        self.created = kwargs.get('created', None)
        self.modified = kwargs.get('modified', None)
        self.description = kwargs.get('description', None)
        self.url = kwargs.get('url', None)
        self.tabName = kwargs.get('tabName', None)
        self.status = kwargs.get('status', None)
        self.mart = kwargs.get('mart', None)
        self.assetDs = kwargs.get('assetDs', None)
        self.colNames = kwargs.get('colNames', None)
        self.length = kwargs.get('length', None)

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    ds = DataSet(id="id", parent="{}", child="{}", blockDs="blockDs", job="job", sampleData="{}",
                 name="name", schema="{}", source="source", storeType="storeType", size=0,
                 created=datetime.now(), modified=datetime.now(), description="description", url="url",
                 tabName="tabName", status="status", mart="mart", assetDs="assetDs", colNames="{}", length=0)
    print(ds)
