# -*- coding: UTF-8 -*-
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session

import traceback
import uuid

Base = declarative_base()


def gen_id():
   return uuid.uuid4().hex




class VituVisitInfo(Base):
    __tablename__ = 't_bigdata_yitu_visit_record'

    def __init__(self):
        super

    id = Column(String(64), default=gen_id, primary_key=True)
    device_id = Column(String(64))
    ci = Column(String(64))
    eci = Column(String(64))
    face_id = Column(String(64))
    score = Column(Float())
    timestamp = Column(Integer())
    is_deal = Column(String(2))


class VituAnalyzeResult(Base):
    __tablename__ = 't_bigdata_yitu_analyze_result'

    id = Column(String(32), default=gen_id, primary_key=True)
    face_id = Column(String(64))
    msisdn = Column(String(32))



class DBEngine():
    def __init__(self):
        self.engine = create_engine("mysql+pymysql://root:Didadi5.com@135.36.245.89:3306/smartcity?charset=utf8", echo=False)
        self._dbSession = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        )

    def commit(self):
        self._dbSession.commit()

    def closeDB(self):
        self._dbSession.commit()
        self._dbSession.flush()
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