# -*- coding: UTF-8 -*-
from jasper_spider import *
from redis_helper import *

def listen_redis():
    try:
        redis_helper = RedisHelper()
        for item in redis_helper.listen(config.JASPER_ACQUIRE_SUBSCRIBE):
            if item['type'] == 'message':
                task_id = str(item['data'], encoding='utf-8')
                print('获取到主题' + config.JASPER_ACQUIRE_SUBSCRIBE + ',消息:' + task_id)
                sync_task = dbutil.query_sync_task(task_id)
                if sync_task:
                    account = dbutil.query_account(sync_task.account_id, 'jasper')
                    user_name = account.login_user
                    password = account.login_password

                    print('获取账户ID:' + account.account_id + ',登录用户:' + user_name)
                    spider = JasperSpider(task_id, user_name, password, config.JASPER_QUERY_LIMIT)
                    spider.start()
    except:
        traceback.print_exc()


listen_redis()