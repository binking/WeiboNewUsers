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
    WEIBO_ACCOUNT_PASSWD, WEIBO_CURRENT_ACCOUNT,
    WEIBO_ERROR_TIME, WEIBO_ACCESS_TIME,
    MANUAL_COOKIES, OUTER_MYSQL, QCLOUD_MYSQL,
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
CURRENT_ACCOUNT = ''

def init_current_account(cache):
    print 'Initializing weibo account'
    global CURRENT_ACCOUNT
    CURRENT_ACCOUNT = cache.hkeys(MANUAL_COOKIES)[0]
    print '1', CURRENT_ACCOUNT
    if not cache.get(WEIBO_CURRENT_ACCOUNT):
        cache.set(WEIBO_CURRENT_ACCOUNT, CURRENT_ACCOUNT)
        cache.set(WEIBO_ACCESS_TIME, 0)
        cache.set(WEIBO_ERROR_TIME, 0)
    

def switch_account(cache):
    global CURRENT_ACCOUNT
    if cache.get(WEIBO_ERROR_TIME) and int(cache.get(WEIBO_ERROR_TIME)) > 9999:  # error count
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), 'Swithching weibo account'
        expired_account = cache.get(WEIBO_CURRENT_ACCOUNT)
        access_times = cache.get(WEIBO_ACCESS_TIME)
        error_times = cache.get(WEIBO_ERROR_TIME)
        print "Account(%s) access %s times but failed %s times" % (expired_account, access_times, error_times)
        cache.hdel(MANUAL_COOKIES, expired_account)
        if len(cache.hkeys(MANUAL_COOKIES)) == 0:
            cache.delete(WEIBO_CURRENT_ACCOUNT)
            cache.set(WEIBO_ACCESS_TIME, 0)
            cache.set(WEIBO_ERROR_TIME, 0)
            raise RedisException('All Weibo Accounts were run out of')
        else:
            new_account = cache.hkeys(MANUAL_COOKIES)[0]
        # init again
        cache.set(WEIBO_CURRENT_ACCOUNT, new_account)
        cache.set(WEIBO_ACCESS_TIME, 0)
        cache.set(WEIBO_ERROR_TIME, 0)
        CURRENT_ACCOUNT = new_account
    elif cache.get(WEIBO_CURRENT_ACCOUNT):
        CURRENT_ACCOUNT = cache.get(WEIBO_CURRENT_ACCOUNT)
    else:
        raise Exception('Unknown Account Error')


def generate_info(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate New User Process pid is %d" % (cp.pid)
        job = cache.blpop(PEOPLE_JOBS_CACHE, 0)[1]
        try:
            if error_count > 999:
                print '>'*10, 'Exceed 1000 times of gen errors', '<'*10
                break
            # switch_account(cache)
            # cache.incr(WEIBO_ACCESS_TIME)
            if cache.sismember(INACTIVE_USER_CACHE, job) or len(job) < 10:
                print 'Inactive user: %s' % job
                continue
            all_account = cache.hkeys(MANUAL_COOKIES)
            account = random.choice(all_account)
            spider = WeiboUserSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            # spider.add_request_header()
            spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            status = spider.gen_html_source()
            if status in [404, 20003]:
                cache.sadd(INACTIVE_USER_CACHE, spider.url)
            res = spider.parse_bozhu_info()
            if res:
                cache.rpush(PEOPLE_RESULTS_CACHE, pickle.dumps(res))
        except RedisException as e:
            print str(e)
            break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to parse job: ', job
            cache.rpush(PEOPLE_JOBS_CACHE, job) # put job back
            # cache.incr(WEIBO_ERROR_TIME)
            error_count += 1
        

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
        res = cache.blpop(PEOPLE_RESULTS_CACHE, 0)[1]
        try:
            dao.insert_new_user_into_db(pickle.loads(res))
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', str((pickle.loads(res)))
            cache.rpush(PEOPLE_RESULTS_CACHE, res)


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
    if not r.llen(PEOPLE_JOBS_CACHE):
        add_jobs(r)
        print "Add jobs DONE, and I quit..."
        return 0
    else:
        print "Redis has %d records in cache" % r.llen(PEOPLE_JOBS_CACHE)
    # init_current_account(r)
    job_pool = mp.Pool(processes=8,
        initializer=generate_info, initargs=(r, ))
    result_pool = mp.Pool(processes=4, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
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
