# -*- coding: utf-8 -*-1

from pyhive import hive
import config
import time
import os
import logging
import gzip
import hbaseutil
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


logger = logging.getLogger("sourceLogger")
logger.setLevel(logging.INFO)
#文件日志保留日志90天
file_log_handler = logging.handlers.TimedRotatingFileHandler(filename=config.log_path+"/load_4g_mc_source.log", when='D', interval=1, backupCount=90)
file_log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
#控制台日志
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
#设置日志handler
logger.addHandler(file_log_handler)
logger.addHandler(console_handler)

target_eci_list = ['46001A015117', '460010D4B30B']
partition_dict = {}
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

def is_target(eci):
    if eci in target_eci_list:
        return True
    return False

def convert_timestamp(date_str):
    timeArray = time.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(timeArray))

# laod gbiups
def load_uniom_mc():
    while True:
        dirs = os.listdir(config.unicom_4g_mc_source)
        for file in dirs:
            file_path = os.path.join(config.unicom_4g_mc_source, file)
            if os.path.isfile(file_path):
                if ':' in file_path:
                    logger.info('发现文件名包含冒号[:]')
                    replace_path = file_path.replace(':', '')
                    logger.info('重命名:%s,%s' % (file_path, replace_path))
                    os.rename(file_path, replace_path)
                    file_path = replace_path
                    file = file.replace(':', '')

                logger.info('开始加载文件:' + file_path)
                date = file.split("_")[1]
                month_part = date[0:4] + date[5:7]
                day_part = date[8:10]

                hb_data_list = []
                for line in gzip.open(file_path,'rt', encoding='utf-8'):
                    file_content = line[0:-2]
                    content_list = file_content.split('|')
                    content_list = content_list[0:9]
                    # content_list.append(month_part)
                    # content_list.append(day_part)
                    eci = content_list[5]
                    if is_target(eci):
                        phone = content_list[1]
                        proc_time = content_list[7]
                        hb_data = {
                            'row_key': eci + ':' + str(convert_timestamp(proc_time)),
                            'data': {
                                'info:eci' : eci,
                                'info:phone' : phone
                            }
                        }
                        hb_data_list.append(hb_data)

                #目标eci入hbase
                if len(hb_data_list) > 0:
                    logger.info('hbase数据入库准备,记录数::' + str(len(hb_data_list)))
                    table = hbaseutil.get_hb_table('unicom_4g_mc_target')
                    for hb_data in hb_data_list:
                        hbaseutil.write_table(table, hb_data['row_key'], hb_data['data'])
                    logger.info('入hbase完成')
                hb_data_list.clear()

                command = 'alter table unicom_4g_mc add if not exists partition (month_part=%s,day_part=%s)' % (month_part, day_part)
                execute_command(command)
                start_time = datetime.now()
                command = "load data local inpath '%s' into table unicom_4g_mc partition(month_part=%s, day_part=%s)" % (file_path, month_part, day_part)
                start_time = datetime.now()
                execute_command(command)
                end_time = datetime.now()
                logger.info('完成加载文件:' + file_path + ',耗时:' + str(end_time - start_time))
                os.rename(file_path, os.path.join(config.unicom_4g_mc_target, file))
        logger.info('扫描目录:' + config.unicom_4g_mc_source)
        time.sleep(5)

if __name__ == '__main__':
    load_uniom_mc()

