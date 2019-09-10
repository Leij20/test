# -*- coding: UTF-8 -*-

import redis


# pool = redis.ConnectionPool(host='135.36.245.41', port=6379, password="creator")
# r = redis.StrictRedis(connection_pool=pool)
#
# def listen_subscribe(sub):
#     p = r.pubsub()
#     p.subscribe(sub)
#     print '开始监听主题:' + sub
#     return p.listen()
#
# def publish_subscribe(sub, message):
#     p = r.pubsub()
#     p.subscribe(sub)
#     print '发布主题:'+ sub + ',消息:' + message

class RedisHelper(object):
    def __init__(self):
        self.__conn = redis.Redis(host='10.81.84.200', port=6379, password='creator')#连接Redis

    def publish(self, channel, msg):
        self.__conn.publish(channel, msg)
        print('发布主题:' + channel + ',消息:' + msg)
        return True

    def subscribe(self, channel):
        pub = self.__conn.pubsub()
        pub.subscribe(channel)
        pub.parse_response()
        return pub

    def listen(self, channel):
        p = self.subscribe(channel)
        print('开始监听主题:' + channel)
        return p.listen()
