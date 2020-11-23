import uuid
from datetime import datetime
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Partner(Base):
    __tablename__ = 'partner'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String)
    address = Column(String)
    phoneNumber = Column(String)
    web = Column(String)
    employee = Column(String, default='{}')
    created = Column(String, default=datetime.now())
    modified = Column(String, default=datetime.now())

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        self.name = kwargs.get('name', None)
        self.address = kwargs.get('address', None)
        self.phoneNumber = kwargs.get('phonenumber', None)
        self.web = kwargs.get('web', None)
        self.employee = kwargs.get('employee', None)
        self.created = kwargs.get('created', None)
        self.modified = kwargs.get('modified', None)

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    pn = Partner(id="id", name="name", address="address", phonenumber="phonenumber", web="web",
                 employee="{}", created=datetime.now(), modified=datetime.now())
    print(pn)
