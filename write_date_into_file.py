#coding=utf-8
import redis
from config.weibo_config import (
    OUTER_MYSQL,
    REDIS_SETTING,
    ACTIVATED_COOKIE,
    WEIBO_ACCOUNT_PASSWD,
    WEIBO_ACCOUNT_LIST,
)
REDIS_USER_LABEL = "weibo:user:label"
def write_data_into_file(rconn):
    """
    Consummer for topics
    """
    # cp = mp.current_process()
    # dao = WeiboWriter(OUTER_MYSQL)
    # while True:
        # print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Follow Process pid is %d" % (cp.pid)
    with open('user_id_label_backup.txt', 'w') as fw:
        for key in rconn.hkeys(REDIS_USER_LABEL):
            user_id, label = key, rconn.hget(REDIS_USER_LABEL, key)
        # print user_id, label
            fw.write("%s\t%s\n" % (user_id, label))
        # info_results.task_done()

r = redis.StrictRedis(**REDIS_SETTING)
write_data_into_file(r)
