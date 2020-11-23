import uuid
from datetime import datetime
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Role(Base):
    __tablename__ = 'role'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String)
    accountRole = Column(String, default='{}')
    description = Column(String)
    scope = Column(String, default='{}')
    created = Column(String, default=datetime.now())
    modified = Column(String, default=datetime.now())

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        self.name = kwargs.get('name', None)
        self.accountRole = kwargs.get('accountrole', None)
        self.description = kwargs.get('description', None)
        self.scope = kwargs.get('scope', None)
        self.created = kwargs.get('created', None)
        self.modified = kwargs.get('modified', None)

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    role = Role(id="id", name="name", accountrole="{}", description="description",
                 scope="{}", created=datetime.now(), modified=datetime.now())
    print(role)
