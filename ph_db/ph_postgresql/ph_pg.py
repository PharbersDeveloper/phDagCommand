import os
import copy
import base64
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PhPg(object):
    def __init__(self):
        self.engine = None
        self.session = None
        #
        # self.host = base64.b64decode(os.getenv('PG_HOST')).decode('utf8')[:-1]
        # self.port = base64.b64decode(os.getenv('PG_PORT')).decode('utf8')[:-1]
        # self.user = base64.b64decode(os.getenv('PG_USER')).decode('utf8')[:-1]
        # self.passwd = base64.b64decode(os.getenv('PG_PASSWD')).decode('utf8')[:-1]
        # self.db = base64.b64decode(os.getenv('PG_DB')).decode('utf8')[:-1]

        self.host = base64.b64decode('cGgtZGItbGFtYmRhLmNuZ2sxamV1cm1udi5yZHMuY24tbm9ydGh3ZXN0LTEuYW1hem9uYXdzLmNvbS5jbgo=').decode('utf8')[:-1]
        self.port = base64.b64decode('NTQzMgo=').decode('utf8')[:-1]
        self.user = base64.b64decode('cGhhcmJlcnMK').decode('utf8')[:-1]
        self.passwd = base64.b64decode('QWJjZGUxOTYxMjUK').decode('utf8')[:-1]
        self.db = base64.b64decode('cGhlbnRyeQo=').decode('utf8')[:-1]

        self.engine = create_engine('postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(**self.__dict__))
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def tables(self):
        """
        列出全部数据表
        :return:
        """
        return self.engine.table_names()

    def query(self, obj):
        """
        查询数据
        :return:
        """
        result = self.session.query(obj.__class__)
        for k, v in obj.__dict__.items():
            if k != '_sa_instance_state' and v:
                result = result.filter(getattr(obj.__class__, k) == v)
        result = result.all()
        return result

    def insert(self, obj):
        """
        插入数据
        :return:
        """
        self.session.add(obj)
        self.session.flush()
        result = copy.deepcopy(obj)
        self.session.commit()
        return result


if __name__ == '__main__':
    from ph_max_auto.ph_models.phentry import DataSet
    pg = PhPg()

    print(pg.tables())

    print(pg.insert(DataSet(job='job')))

    query = pg.query(DataSet(job="job"))
    for q in query:
        print(q)

