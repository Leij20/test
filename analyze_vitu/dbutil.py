# -*- coding: UTF-8 -*-

from dbengine import *

dbengine = DBEngine()

#查询接口表数据
def query_visit_list(is_deal=0):
    try:
        return dbengine._dbSession.query(VituVisitInfo).filter(VituVisitInfo.is_deal == is_deal).limit(10)
    except Exception as e:
        raise e


#按时间排序，查询相同face_id，最近的记录数据
def query_his_visit_list(face_id, limit=30):
    try:
        return dbengine._dbSession.query(VituVisitInfo).filter(and_(VituVisitInfo.is_deal == '1', VituVisitInfo.face_id == face_id)).order_by(VituVisitInfo.timestamp.desc()).limit(limit)
    except Exception as e:
        raise e


def change_visit_finish(visit):
    try:
        visit_info =  dbengine._dbSession.query(VituVisitInfo).filter(VituVisitInfo.id == visit.id).first()
        if visit_info:
            visit_info.is_deal = 1
            dbengine._dbSession.merge(visit_info)
    except Exception as e:
        raise e
    finally:
        dbengine.commit()

#保存分析结果
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
