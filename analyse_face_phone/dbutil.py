# -*- coding:utf-8 -*-
from dbengine import *

dbengine = DBEngine()

#查询接口表数据
def query_visit_list(is_deal = 0):
    try:
        return dbengine._dbSession.query(VituVisitInfo).filter(VituVisitInfo.is_deal == is_deal).limit(10)
    except Exception as e:
        raise e

#按时间排序，查询相同face_id，最近的记录数据
def query_his_visit_list(face_id,limit=30):
    try:
        return dbengine._dbSession.query(VituVisitInfo).filter(and_(VituVisitInfo.is_deal == '1',VituVisitInfo.face_id == face_id)).order_by(VituVisitInfo.timestamp.desc()).limit(limit)
    except Exception as e:
        raise e


def save_analyze_result(result):
    try:
        dbengine = DBEngine()
        if result is None:
            return
        dbengine._dbSession.add(result)
    except Exception as e:
        raise e
    finally:
        dbengine.commit()
