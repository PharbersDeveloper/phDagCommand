import os
import copy
import base64
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PhPg(object):
    def __init__(self, host, port, user, passwd, db):
        self.engine = None
        self.session = None

        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db

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

    def delete(self, obj):
        result = self.query(obj)
        for r in result:
            self.session.delete(r)
        self.session.commit()
        return result

    def update(self, obj):
        tmp = obj.__dict__
        del tmp['_sa_instance_state']
        obj_id = tmp.pop('id')
        tmp = dict([(t, tmp[t]) for t in tmp if tmp[t]])

        # 如果没有要更新的元素，直接返回
        if not tmp:
            return obj

        result = self.session.query(obj.__class__).filter(getattr(obj.__class__, 'id') == obj_id)
        result.update(tmp)
        self.session.commit()
        return obj


if __name__ == '__main__':
    from ph_max_auto.ph_models.data_set import DataSet

    pg = PhPg(
        base64.b64decode(os.getenv('PG_HOST')).decode('utf8')[:-1],
        base64.b64decode(os.getenv('PG_PORT')).decode('utf8')[:-1],
        base64.b64decode(os.getenv('PG_USER')).decode('utf8')[:-1],
        base64.b64decode(os.getenv('PG_PASSWD')).decode('utf8')[:-1],
        base64.b64decode(os.getenv('PG_DB')).decode('utf8')[:-1],
    )

    print(pg.tables())

    print(pg.insert(DataSet(job='job')))

    query = pg.query(DataSet(job="job"))
    for q in query:
        print(q)

