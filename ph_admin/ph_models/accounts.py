import uuid
from datetime import datetime
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Account(Base):
    __tablename__ = 'account'

    id = Column(String, primary_key=True, default=str(uuid.uuid4()))
    name = Column(String)
    wechatOpenId = Column(String)
    password = Column(String)
    phoneNumber = Column(String)
    defaultRole = Column(String, default="{}")
    email = Column(String, default='{}')
    employer = Column(String, default='{}')
    created = Column(String, default=datetime.now())
    modified = Column(String, default=datetime.now())
    firstName = Column(String, default='{}')
    lastName = Column(String, default='{}')

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', None)
        self.name = kwargs.get('name', None)
        self.wechatOpenId = kwargs.get('wechatopenid', None)
        self.password = kwargs.get('password', None)
        self.phoneNumber = kwargs.get('phonenumber', None)
        self.defaultRole = kwargs.get('defaultRole', None)
        self.email = kwargs.get('email', None)
        self.employer = kwargs.get('employer', None)
        self.created = kwargs.get('created', None)
        self.modified = kwargs.get('modified', None)
        self.firstName = kwargs.get('firstname', None)
        self.lastName = kwargs.get('lastname', None)

    def __str__(self):
        return str(self.__dict__)


if __name__ == '__main__':
    a = Account(id="id", name="name", wechatopenid="wechatopenid", password="password", phonenumber="phonenumber",
                 defaultrole="defaultrole", email='email', employer='employer',
                 created=datetime.now(), modified=datetime.now(), firstnam='firstname', lastname='lastname')
    print(a)
