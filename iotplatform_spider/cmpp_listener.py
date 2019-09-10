# -*- coding: UTF-8 -*-
from cmpp_spider import *
from redis_helper import *

def listen_redis():
    try:
        redis_helper = RedisHelper()
        for item in redis_helper.listen(config.CMPP_ACQUIRE_SUBSCRIBE):
            if item['type'] == 'message':
                task_id = str(item['data'], encoding='utf-8')
                print('获取到主题' + config.CMPP_ACQUIRE_SUBSCRIBE + ',消息:' + task_id)
                sync_task = dbutil.query_sync_task(task_id)
                if sync_task:
                    account = dbutil.query_account(sync_task.account_id, 'cmpp')
                    if account is None:
                        print('同步任务:%s,未获取到对应账户信息' % task_id)
                        continue
                    user_name = account.login_user
                    password = account.login_password

                    print('获取账户ID:' + account.account_id + ',登录用户:' + user_name)
                    spider = CmppSpider(task_id, user_name, password)
                    spider.start()
    except:
        traceback.print_exc()


listen_redis()