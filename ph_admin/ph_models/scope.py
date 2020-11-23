import uuid
from datetime import datetime
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Scope(Base):
    __tablename__ = 'scope'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String)
    description = Column(String)
    scopePolicy = Column(String)
    owner = Column(String, default='{}')
    created = Column(String, default=datetime.now())
    modified = Column(String, default=datetime.now())

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        self.name = kwargs.get('name', None)
        self.description = kwargs.get('description', None)
        self.scopePolicy = kwargs.get('scopepolicy', None)
        self.owner = kwargs.get('owner', None)
        self.created = kwargs.get('created', None)
        self.modified = kwargs.get('modified', None)

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    scope = Scope(id="id", name="name", description="description", scopepolicy="scopepolicy",
                 owner="{}", created=datetime.now(), modified=datetime.now())
    print(scope)
