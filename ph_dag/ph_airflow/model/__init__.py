import uuid
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy import Column, String, ForeignKey, INTEGER
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Dag(Base):
    __tablename__ = 'dag'

    dag_id = Column(String, primary_key=True)
    is_paused = Column(INTEGER)
    is_subdag = Column(INTEGER)
    is_active = Column(INTEGER)
    last_scheduler_run = Column(String, default=datetime.now())
    last_pickled = Column(String, default=datetime.now())
    last_expired = Column(String, default=datetime.now())
    scheduler_lock = Column(INTEGER)
    pickle_id = Column(INTEGER)
    fileloc = Column(String)
    owners = Column(String)
    description = Column(String)
    default_view = Column(String)
    schedule_interval = Column(String)
    root_dag_id = Column(String)
    next_dagrun = Column(String, default=datetime.now())
    next_dagrun_create_after = Column(String, default=datetime.now())
    concurrency = Column(INTEGER)
    has_task_concurrency_limits = Column(INTEGER)

    def __repr__(self):
        return str(self.__dict__)
