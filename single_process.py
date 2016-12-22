#coding=utf-8
import os
import sys
import time
import redis
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from template.weibo_config import (
    WEIBO_MANUAL_COOKIES, WEIBO_ACCOUNT_PASSWD,
    MANUAL_COOKIES, OUTER_MYSQL, QCLOUD_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from template.weibo_utils import create_processes, pick_rand_ele_from_list
from weibo_user_spider import WeiboUserSpider
from template.weibo_user_writer import WeiboUserWriter
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_DATABASE = OUTER_MYSQL
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_DATABASE = QCLOUD_MYSQL
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")


def single_process():
    rconn = redis.StrictRedis(**USED_REDIS)
    dao = WeiboWriter(USED_DATABASE)
    jobs = [ 'http://weibo.com/1681897083/info',  # Yes
            'http://weibo.com/6006659783/info', # Yes
            'http://weibo.com/wendujianzao/info', # NO
            'http://weibo.com/bbjkxf/info',  # NO
            'http://weibo.com/1741566651/info']  # Yes
    for job in jobs:  # iterate
        all_account = rconn.hkeys(MANUAL_COOKIES)
        if not all_account:  # no any weibo account
            raise Exception('All of your accounts were Freezed')
        account = pick_rand_ele_from_list(all_account)
        # operate spider
        spider = WeiboUserSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
        spider.use_abuyun_proxy()
        spider.add_request_header()
        spider.use_cookie_from_curl(rconn.hget(MANUAL_COOKIES, account))
        spider.gen_html_source()
        res = spider.parse_bozhu_info()
        print res
        dao.insert_new_user_into_db(res)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
