# -*- coding: UTF-8 -*-

from dbengine import *


#查询需要获取数据的同步任务
def query_need_sync_task():
    dbEngine = DBEngine()
    try:
        result = dbEngine._dbSession.query(CardSyncTask).filter(CardSyncTask.status == '0')
        return result
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()

def clean_sync_task_detail(task_id):
    dbEngine = DBEngine()
    try:
        dbEngine._dbSession.query(CardSyncDetail).filter(CardSyncDetail.task_id == task_id).delete()
        dbEngine._dbSession.commit()
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()

def query_sync_task(id):
    dbEngine = DBEngine()
    try:
        result = dbEngine._dbSession.query(CardSyncTask).filter(CardSyncTask.id == id)
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()

def save(object):
    dbEngine = DBEngine()
    try:
        dbEngine.save(object)
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()

def batchSave(objects):
    dbEngine = DBEngine()
    try:
        dbEngine.batchSave(objects)
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()

def query_account(account_id, platform):
    dbEngine = DBEngine()
    try:
        return  dbEngine._dbSession.query(Account).filter(Account.account_id == account_id, Account.interface_platform == platform).scalar()
    except Exception as e:
        traceback.print_stack()
    finally:
        dbEngine.closeDB()
        dbEngine.remove()