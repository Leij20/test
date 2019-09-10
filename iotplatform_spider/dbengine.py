# -*- coding: UTF-8 -*-

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
import os
import os.path
import traceback
import uuid



os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
Base = declarative_base()


def gen_id():
   return uuid.uuid4().hex


class CardSyncTask(Base):
    __tablename__ = 't_b_card_synctask'

    def __init__(self, info):
        super

    id = Column(String(32), primary_key=True)
    account_id = Column(String(32))
    account_name = Column(String(32))
    status = Column(String(1))
    need_sync_number = Column(Integer)
    finish_sync_number = Column(Integer)
    start_date = Column(DateTime())
    end_date = Column(DateTime())
    create_name = Column(String(50))
    create_by = Column(String(50))
    create_date = Column(DateTime())
    update_name = Column(String(50))
    update_by = Column(String(50))
    update_date = Column(DateTime())
    sys_company_code = Column(String(50))
    sys_org_code = Column(String(50))


class CardSyncDetail(Base):
    __tablename__ = 't_b_card_syncdetail'

    id = Column(String(32), default=gen_id, primary_key=True)
    task_id = Column(String(32))
    iccid = Column(String(32))
    status = Column(String(1))


class Account(Base):
    __tablename__ = 't_b_account'

    id = Column(String(32), primary_key=True)
    account_id = Column(String(32))
    account_name = Column(String(32))
    operator_name = Column(String(64))
    company_name = Column(String(64))
    login_user = Column(String(32))
    login_password = Column(String(32))
    interface_platform = Column(String(32))
    interface_status = Column(String(1))
    api_user = Column(String(64))
    api_password = Column(String(64))
    api_key = Column(String(64))
    receive_api_key = Column(String(64))
    remark = Column(String(64))

class DBEngine():
    def __init__(self):
        self.engine = create_engine("mysql+pymysql://root:didadi@10.81.84.200:8889/iotmanager", echo=True)
        #self.engine = create_engine("mysql+pymysql://root:123456@localhost:3306/iotmanager", echo=False)
        self._dbSession = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        )

    def closeDB(self):
        self._dbSession.close()


    def batchSave(self, objects):
        try:
            for object in objects:
                self._dbSession.merge(object)
        except:
            traceback.print_exc()
            self.rollback()
        finally:
            self._dbSession.commit()

    def save(self, object):
        try:
            if not object:
                return
            self._dbSession.merge(object)
        except Exception as e:
            traceback.print_exc()
        finally:
            self._dbSession.commit()

    def rollback(self):
        self._dbSession.rollback()

    def remove(self):
        self._dbSession.remove()

