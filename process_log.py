#coding=utf-8
import re
import os
import redis
from template.weibo_config import LOCAL_REDIS, QCLOUD_REDIS


INACTIVE_USER = 'weibo:inactive:users'

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")

def extract_user_url(filename):
    users = set()
    with open(filename, 'r') as fr:
        for line in fr.readlines():
            if '抱歉，您当前访问的帐号异常，暂时无法访问' in line \
            or '抱歉，你访问的页面地址有误，或者该页面不存在' in line:
                mat = re.search(r'(http://.+?/info)', line)
                if mat:
                    users.add(mat.group(1))
    return users

if __name__=='__main__':
    r = redis.StrictRedis(**USED_REDIS)
    logs = ['log/new_user_2016-12-22.log', 'log/new_user_2016-12-23.log']
    inactive_users = set()
    for log in logs :
        print log
        res = extract_user_url(log)
        inactive_users.update(res)
    for user in inactive_users:
        if len(user) > 50:
            continue
        r.sadd(INACTIVE_USER, user)
    print 'DONE'