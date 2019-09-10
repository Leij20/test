# -*- coding: UTF-8 -*-

from pyhive import hive
import config
import time
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import dbutil
from dbutil import VituAnalyzeResult
import pandas as pd
import hbaseutil

logger = logging.getLogger("analyzePhoneLogger")
logger.setLevel(logging.INFO)
#文件日志保留日志90天
file_log_handler = logging.handlers.TimedRotatingFileHandler(filename=config.log_path+"/analyze_phone.log", when='D', interval=1, backupCount=90)
file_log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
#控制台日志
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#设置日志handler
logger.addHandler(file_log_handler)
logger.addHandler(console_handler)

hive_conn = None
def get_hive_conn():
    global hive_conn
    if hive_conn is None:
        hive_conn = hive.Connection(host=config.host, port=config.port, username='', database=config.database)
    return hive_conn

def execute_command(command):
    conn = get_hive_conn()
    cursor = conn.cursor()
    logger.info('执行hive:' + command)
    cursor.execute(command)
    return cursor

def sacn_data(visit, setp_seconds=1800):
    timestamp = datetime.datetime.utcfromtimestamp(visit.timestamp)
    start_time = visit.timestamp - setp_seconds
    end_time = visit.timestamp + setp_seconds
    scan_filter = "SingleColumnValueFilter('info', 'eci', =, 'substring:" + visit.eci + "', true, true) "
    row_start = str(visit.eci) + ':' + str(start_time)
    row_stop = str(visit.eci) + ':' + str(end_time)
    logger.info('分析eci:' + str(visit.eci) + ',开始时间:' + str(start_time) + ',结束时间:' + str(end_time))
    table = hbaseutil.get_hb_table('unicom_4g_mc_target')
    data = table.scan(row_start=row_start, row_stop=row_stop, columns=[b'info:eci', b'info:phone'], filter=scan_filter)
    eci_list = []
    phone_list = []
    for k, v in data:
        # eci_list.append(v[b'info:eci'].decode())
        phone = v[b'info:phone'].decode()
        if phone is not None and len(phone) > 0:
            phone_list.append(v[b'info:phone'].decode())
    data_dict = {
        # 'eci': eci_list,
        'phone': phone_list
    }
    df = pd.DataFrame.from_dict(data_dict)
    df = df.drop_duplicates()
    logger.info('分析eci:' + visit.eci + ',结果数:' + str(len(df)))

    return df

def analyze(visit_list):
    if visit_list is None:
        return
    df = None
    result_list = []
    for visit in visit_list:
        data = sacn_data(visit)
        if data is None:
            return None
        if df is None:
            df = data
        else:
            df = pd.merge(df, data)
            logger.info('合并后结果:' + str(len(df)))
            print(df)
        if len(df) == 1:
                phone = str(df.iloc[0,0])
                print(phone)
                logger.info('分析结果,手机号码:' + phone)
                result_list.append(phone)

    return result_list


def scan_visit():
    visit_list = dbutil.query_visit_list()
    while True:
        source_list = []
        visit_list = dbutil.query_visit_list() #查询视频访问数据
        if visit_list is None:
            continue
        for visit in visit_list:
            logger.info('分析视频记录开始,id:' + visit.id + ',eci:' + visit.eci)
            source_list.append(visit)
            face_id = visit.face_id
            his_visit_list = dbutil.query_his_visit_list(face_id)
            if his_visit_list is None:
                continue
            for his_visit in his_visit_list:
                source_list.append(his_visit)

            result_list = analyze(source_list)
            if result_list:
                for result in result_list:
                    analyze_result = VituAnalyzeResult()
                    analyze_result.face_id = visit.face_id
                    analyze_result.msisdn = result
                    dbutil.save_analyze_result(analyze_result)
            dbutil.change_visit_finish(visit)  #设置处理完成标识
            logger.info('分析视频记录完成,id:' + visit.id + ',eci:' + visit.eci)
        time.sleep(1)

if __name__ == '__main__':
    scan_visit()


