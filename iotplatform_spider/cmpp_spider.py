# -*- coding: UTF-8 -*-

from PIL import Image
from io import BytesIO
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
from cmpp_decoder import *

requests.adapters.DEFAULT_RETRIES = 5

class CmppSpider(threading.Thread):

    def __init__(self, task_id, user_name, password):
        threading.Thread.__init__(self)
        self.task_id = task_id
        self.user_name = user_name
        self.password = password
        self.session = requests.session()
        self.session.headers = {
            'Accept': 'image/png, image/svg+xml, image/jxr, image/*; q=0.8, */*; q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN',
            'Connection': 'keep-alive',
            'Host': '39.130.150.21:8081',
            'Referer': 'http://39.130.150.21:8081/login',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko'
        }
        self.is_login = False
        self.count = 0
        self.pages = 0
        self.currentPage = 1

    def get_image_code(self):
        try:
            url = 'http://39.130.150.21:8081/getImageCode?t=1,535,446,241,824'
            r = self.session.get(url)
            if r.status_code == 200:
                image = Image.open(BytesIO(r.content))
                image_code = verify_code(image)
                print('识别出的图形验证码为：%s' % image_code)
                return image_code
        except Exception as e:
            traceback.print_exc()
        finally:
            self.session.close()


    def check_user(self):
        try:
            url = 'http://39.130.150.21:8081/checkUser'
            image_code = self.get_image_code()
            index = 1
            while image_code is None or len(image_code) == 0:
                print('识别图形验证码失败，进行%d次重试' % index)
                image_code = self.get_image_code
                index += 1

            data = {
                'account': self.user_name,
                'imageCode': image_code,
                'password': self.password
            }
            r = self.session.post(url, data)
            if r.status_code == 200:
                result = json.loads(r.text)
                if result['status'] == 1:
                    print('用户检查成功')
                else:
                    print('用户检查失败，重试')
                    self.check_user()
            else:
                print('请求返回状态失败')

        except Exception as e:
            traceback.print_exc()
        finally:
            self.session.close()



    def login(self):
        try:
            self.check_user()
            url = 'http://39.130.150.21:8081/checkFirstLogin'
            data = {
                'account': self.user_name,
                'password': self.password
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

    def get_total_info(self):
        try:
            url = 'http://39.130.150.21:8081/card/cardJson?state=0'
            data = {
                'page': 1,
                'rows': '100',
                'sord': 'asc'
            }
            r = self.session.post(url, data)

            result = json.loads(r.text)
            if 'total' in result.keys() and 'records' in result.keys():
                info = {
                    'total': int(result['total']),
                    'records' : int(result['records'])
                }
                return info
        except Exception as e:
            traceback.print_exc()
        finally:
            self.session.close()

    def search_by_page(self, page):
        try:
            url = 'http://39.130.150.21:8081/card/cardJson?state=0'
            data = {
                '_search': 'false',
                'cycleState': '0',
                'page': page,
                'rows': '100',
                'sord': 'asc'
            }
            r = self.session.post(url, data)
            result = json.loads(r.text)
            if 'rows' in result.keys():
                data = result['rows']
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

        total_info = self.get_total_info();
        self.count = total_info['records']
        self.pages = total_info['total']

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
        RedisHelper().publish(config.CMPP_SYNC_CARD_SUBSCRIBE, self.task_id)