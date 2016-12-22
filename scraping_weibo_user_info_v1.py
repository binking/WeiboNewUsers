#coding=utf-8
import os
import sys
import time
import redis
import argparse
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from requests.exceptions import ConnectionError
from config.weibo_config import (
    QCLOUD_MYSQL,
    OUTER_MYSQL,
    WEIBO_ACCOUNT_LIST, 
    WEIBO_ACCOUNT_PASSWD, 
    ACTIVATED_COOKIE,
    LOCAL_REDIS,
    QCLOUD_REDIS
)
from utils.weibo_utils import (
    create_processes,
    pick_rand_ele_from_list
)
from weibo_bozhu_info_spider import BozhuInfoSpider
from dao.weibo_writer import WeiboWriter
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_DATABASE = OUTER_MYSQL
    USED_REDIS = LOCAL_REDIS
elif os.environ.get('HOSTNAME') == 'VM_20_202_centos': 
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
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Bozhu Process pid is %d" % (cp.pid)
        job = jobs.get()
        all_account = rconn.hkeys(ACTIVATED_COOKIE)
        if not all_account:  # no any weibo account
            raise Exception('All of your accounts were Freezed')
        auth = pick_rand_ele_from_list(all_account)
        account, password = auth.split('--')
        # operate spider
        spider = BozhuInfoSpider(job, account, password, timeout=20)
        spider.use_abuyun_proxy()
        spider.add_request_header()
        spider.read_cookie(rconn)
        try:
            spider.gen_html_source()
        except Exception as e:
            print str(e)
            jobs.put(job) # put job back
        if not spider.check_abnormal_status():
            # spider.remove_cookie(rconn)
            res = spider.parse_bozhu_info()
            if res:
                results.put(res)
        jobs.task_done()


def user_db_writer(results):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    # dao = WeiboWriter(OUTER_MYSQL)
    dao = WeiboWriter(USED_DATABASE)
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Bozhu Process pid is %d" % (cp.pid)
        res = results.get()
        # print info_dict
        dao.insert_new_user_into_db(res)
        results.task_done()


def add_jobs(target):
    todo = 0
    # dao = WeiboWriter(OUTER_MYSQL)
    dao = WeiboWriter(USED_DATABASE)
    jobs = dao.read_new_user_from_db()
    for job in jobs:  # iterate
        todo += 1
        try:
            target.put(job)
            if todo > 10:
                break
        except Exception as e:
            print e
    return todo

def run_all_worker():
    try:
        # load weibo account into redis cache
        # r = redis.StrictRedis(**LOCAL_REDIS)
        r = redis.StrictRedis(**USED_REDIS)
        # Producer is on !!!
        jobs = mp.JoinableQueue()
        results = mp.JoinableQueue()
        create_processes(user_info_generator, (jobs, results, r), 4)
        create_processes(user_db_writer, (results, ), 8)

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


def single_process():
    rconn = redis.StrictRedis(**USED_REDIS)
    todo = 0
    # dao = WeiboWriter(OUTER_MYSQL)
    dao = WeiboWriter(USED_DATABASE)
    jobs = dao.read_new_user_from_db()
    for job in jobs:  # iterate
        todo += 1
        all_account = rconn.hkeys(ACTIVATED_COOKIE)
        if not all_account:  # no any weibo account
            raise Exception('All of your accounts were Freezed')
        auth = pick_rand_ele_from_list(all_account)
        account, password = auth.split('--')
        # operate spider
        spider = BozhuInfoSpider(job, account, password, timeout=20)
        spider.use_abuyun_proxy()
        spider.add_request_header()
        spider.read_cookie(rconn)
        try:
            spider.gen_html_source()
        except Exception as e:
            print str(e)
            jobs.put(job) # put job back
        if not spider.check_abnormal_status():
            res = spider.parse_bozhu_info()
            print res
        if todo > 2:
            break

if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    # run_all_worker()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
