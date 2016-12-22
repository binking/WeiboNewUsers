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


def user_info_generator(jobs, results, rconn):
    """
    Producer for urls and topics, Consummer for topics
    """
    cp = mp.current_process()
    while True:
        res = {}
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate New User Process pid is %d" % (cp.pid)
        try:
            job = jobs.get()
            all_account = rconn.hkeys(MANUAL_COOKIES)
            if not all_account:  # no any weibo account
                raise Exception('All of your accounts were Freezed')
            account = pick_rand_ele_from_list(all_account)
            spider = WeiboUserSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            spider.use_cookie_from_curl(rconn.hget(MANUAL_COOKIES, account))
        
            spider.gen_html_source()
            res = spider.parse_bozhu_info()
            if res:
                results.put(res)
        except Exception as e:  # no matter what was raised, cannot let process died
            print 'Raised in gen process', str(e)
            jobs.put(job) # put job back
        jobs.task_done()


def user_db_writer(results):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    dao = WeiboUserWriter(USED_DATABASE)
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write New User Process pid is %d" % (cp.pid)
        try:
            res = results.get()
            dao.insert_new_user_into_db(res)
        except Exception as e:  # won't let you died
            print 'Raised in gen process', str(e)
            results.put(res)
        results.task_done()

def add_jobs(target):
    todo = 0
    dao = WeiboUserWriter(USED_DATABASE)
    jobs = dao.read_new_user_from_db()
    for job in jobs:  # iterate
        todo += 1
        target.put(job)
    return todo

def run_all_worker():
    try:
        r = redis.StrictRedis(**USED_REDIS)
        # Producer is on !!!
        jobs = mp.JoinableQueue()
        results = mp.JoinableQueue()
        create_processes(user_info_generator, (jobs, results, r), 4)
        create_processes(user_db_writer, (results, ), 4)

        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        num_of_users = add_jobs(jobs)
        print "<"*10,
        print "There are %d users to process" % num_of_users, 
        print ">"*10
        jobs.join()
        results.join()
        print "+"*10, "jobs' length is ", jobs.qsize()
        print "+"*10, "results' length is ", results.qsize()
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in runn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", jobs.qsize()
        print "+"*10, "results' length is ", results.qsize()


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    # single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
