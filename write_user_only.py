#coding=utf-8
import os
import sys
import time
import redis
import pickle
import random
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from zc_spider.weibo_config import (
    WEIBO_ACCOUNT_PASSWD, WEIBO_COOKIES, 
    OUTER_MYSQL, QCLOUD_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS, INACTIVE_USER_CACHE,
    PEOPLE_JOBS_CACHE, PEOPLE_RESULTS_CACHE  # weibo:people:urls, weibo:people:info
)
from zc_spider.weibo_utils import RedisException
from weibo_user_spider import WeiboUserSpider
from weibo_user_writer import WeiboUserWriter
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

def write_data(cache):
    """
    Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    dao = WeiboUserWriter(USED_DATABASE)
    while True:
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of write errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write New User Process pid is %d" % (cp.pid)
        print "Why stop ?? --> %d" % cache.llen(PEOPLE_RESULTS_CACHE)
        res = cache.blpop(PEOPLE_RESULTS_CACHE, 0)[1]
        data = pickle.loads(res)
        try:
            dao.insert_new_user_into_db(data)
        except Exception as e:  # won't let you died
            traceback.print_exc()
            error_count += 1
            print 'Failed to write result: \n', 
            for k,v in data.items():
                print k, 
                print v
            print "+" * 30
            data['introduction'] = ''
            cache.rpush(PEOPLE_RESULTS_CACHE, pickle.dumps(data))


def add_jobs(target):
    todo = 0
    dao = WeiboUserWriter(USED_DATABASE)
    for job in dao.read_new_user_from_db():  # iterate
        todo += 1
        target.rpush(PEOPLE_JOBS_CACHE, job)
    print 'There are totally %d jobs to process' % todo
    return todo


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    print "Redis has %d records in cache" % r.llen(PEOPLE_JOBS_CACHE)
    result_pool = mp.Pool(processes=8,
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        result_pool.close()
        result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(PEOPLE_JOBS_CACHE) 
        print "+"*10, "results' length is ", r.llen(PEOPLE_RESULTS_CACHE)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", r.llen(PEOPLE_JOBS_CACHE) 
        print "+"*10, "results' length is ", r.llen(PEOPLE_RESULTS_CACHE)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
