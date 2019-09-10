# -*- coding: UTF-8 -*-

import threading
import requests
import json
import traceback
import math
import dbutil
import time
import config
from dbengine import CardSyncDetail
from redis_helper import *

requests.adapters.DEFAULT_RETRIES = 5

class JasperSpider(threading.Thread):

    def __init__(self, task_id, user_name, password, limit):
        threading.Thread.__init__(self)
        self.task_id = task_id
        self.user_name = user_name
        self.password = password
        self.limit = limit
        self.session = requests.session()
        self.session.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Length': '40',
            'Content-Type': 'application/x-www-form-urlencoded',
            #'Cookie': 'jasperLoginFromGwy=true; JSESSIONID=02CE255691C98DED6A92CFA6E660ADE8;jsSessionCookie=123',
            'Host': 'cc2.10646.cn',
            'Origin': 'null',
            'Pragma': 'no-cache',
            'Referer': 'https://m2m.10646.cn/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.352'
        }
        self.is_login = False
        self.count = 0
        self.pages = 0
        self.currentPage = 1

    def login(self):
        try:
            url = 'https://cc2.10646.cn/provision/j_acegi_security_check'
            data = {
                'j_username': self.user_name,
                'j_password': self.password
            }
            r = self.session.post(url, data)
            if r.status_code == 200:
                print('登陆成功')
                return True
            else:
                print('登陆失败')
                return False
        except Exception as e:
            print('登陆失败')
            traceback.print_exc()
            return False
        finally:
            self.session.close()

    def search_count(self):
        try:
            url = 'https://cc2.10646.cn/provision/api/v1/sims/searchCount?_dc=1535527662595&search=%5B%5D'
            r = self.session.get(url)

            if r.status_code == 200:
                result = json.loads(r.text)
                return result["totalCount"]
            else:
                print(r.status_code)
                return 0
        except Exception as e:
            traceback.print_exc()
        finally:
            self.session.close()


    def search_by_page(self, page):
        try:
            url = 'https://cc2.10646.cn/provision/api/v1/sims?page='+str(page) + '&limit=' + str(self.limit)
            print(url)
            r = self.session.get(url)
            result = json.loads(r.text)
            if 'data' in result.keys():
                data = result['data']
                return data
        except Exception as e:
            traceback.print_exc()
        finally:
            self.session.close()


    def run(self):
        print('获取同步任务:' + self.task_id + '数据开始')
        start_time = time.time()
        if not self.is_login:
            self.is_login = self.login()

        self.count = self.search_count()
        self.pages = int(math.ceil(self.count / float(self.limit)))

        print('获取同步任务:' + self.task_id + ',需同步数据:' + str(self.count))

        task = dbutil.query_sync_task(self.task_id)
        if not task:
            return
        task.status = 1
        task.need_sync_number = self.count
        dbutil.save(task)
        dbutil.clean_sync_task_detail(self.task_id)

        try:
            while self.currentPage <= self.pages:
                data_list = self.search_by_page(self.currentPage)
                details = []
                for data in data_list:
                    detail = CardSyncDetail()
                    detail.task_id = self.task_id
                    detail.iccid = data['iccid']
                    detail.status = '0'
                    details.append(detail)
                dbutil.batchSave(details)
                self.currentPage += 1
        except Exception as e:
            task.status = 2
            dbutil.save(task)
            traceback.print_exc()
        task.status = 3
        dbutil.save(task)

        end_time = time.time()
        print('获取同步任务:' + self.task_id + '数据结束，共耗时:' + str(end_time - start_time))
        RedisHelper().publish(config.JASPER_SYNC_CARD_SUBSCRIBE, self.task_id)
