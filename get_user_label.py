#coding=utf-8
import os
import sys
import time
import xlrd
import redis
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from utils import create_processes
from config.weibo_config import (
    OUTER_MYSQL,
    REDIS_SETTING,
    ACTIVATED_COOKIE,
    WEIBO_ACCOUNT_PASSWD
)
from utils import pick_rand_ele_from_list
from utils.weibo_utils import gen_cookie
from dao.weibo_writer import WeiboWriter
from spider.weibo_spider import BozhuInfoSpider

REDIS_USER_LABEL = "weibo:user:label"
reload(sys)
sys.setdefaultencoding('utf-8')


def user_info_generator(user_jobs, info_results, rconn):
    """
    Producer for urls and topics, Consummer for topics
    """
    cp = mp.current_process()
    try:
        while True:
            label = ''
            print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Follows Process pid is %d" % (cp.pid)
            user_id = user_jobs.get()
            # all_account = rconn.hkeys(ACTIVATED_COOKIE)
            # if not all_account:  # no any weibo account
            #     raise Exception('All of your accounts were Freezed')
            auth = 'm0257361yingchen@163.com--tttt5555'
            account, password = auth.split('--')
            # operate spider
            spider = BozhuInfoSpider('http://m.weibo.cn/container/getIndex?containerid=230283%s_-_INFO' % user_id, account, WEIBO_ACCOUNT_PASSWD)
            # spider.use_abuyun_proxy()
            spider.read_cookie(rconn)
            try:
                # spider.use_abuyun_proxy()
                spider.add_request_header()
                spider.gen_html_source()
                label = spider.get_user_label()
                print '><'*20, label
            except Exception as e:
                print e
                user_jobs.put(user_id) # put job back
            if not label:
                print "Lalalalala, Cannot get user %s label." % user_id
                # user_jobs.put(user_id) # put job back
            else:
                info_results.put((user_id, label))
            user_jobs.task_done()
    except KeyboardInterrupt as e:
        print str(e)


def user_db_writer(info_results, rconn):
    """
    Consummer for topics
    """
    cp = mp.current_process()
    # ao = WeiboWriter(OUTER_MYSQL)
    while True:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Follow Process pid is %d" % (cp.pid)
        user, label = info_results.get()
        rconn.hset(REDIS_USER_LABEL, user, label)
        info_results.task_done()

def write_data_into_file(rconn):
    """
    Consummer for topics
    """
    # cp = mp.current_process()
    # dao = WeiboWriter(OUTER_MYSQL)
    # while True:
        # print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Follow Process pid is %d" % (cp.pid)
    with open('user_id_label.txt', 'w') as fw:
        for key in rconn.hkeys(REDIS_USER_LABEL):
            user_id, label = key, rconn.hget(REDIS_USER_LABEL, key)
        # print user_id, label
            fw.write("%s\t%s\n" % (user_id, label))
        # info_results.task_done()


def add_user_jobs(target):
    todo = 0
    book = xlrd.open_workbook('buzhuashuju.xlsx')
    # import ipdb; ipdb.set_trace()
    sheet = book.sheet_by_index(0)    
    for i in range(1, sheet.nrows):
        url = sheet.cell_value(i, 1)
        target.put(str(int(url)))
        todo += 1
        # if todo >= 5:
        #     break
    return todo

def init_cookie(rconn):
    for account in WEIBO_ACCOUNT_LIST[::-1][:5]:
        auth = '%s--%s' % (account, WEIBO_ACCOUNT_PASSWD)
        cookie = gen_cookie(account, WEIBO_ACCOUNT_PASSWD)
        if cookie:
            rconn.hset(ACTIVATED_COOKIE, auth, cookie)
            time.sleep(2)

def run_all_worker():
     # load weibo account into redis cache
    r = redis.StrictRedis(**REDIS_SETTING)
    try:
        # init_cookie(r)
        # raise Exception('jiang')
        # Producer is on !!!
        user_jobs = mp.JoinableQueue()
        info_results = mp.JoinableQueue()
        create_processes(user_info_generator, (user_jobs, info_results, r), 2)
        create_processes(user_db_writer, (info_results, r), 1)

        cp = mp.current_process()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
        num_of_users = add_user_jobs(user_jobs)
        print "<"*10,
        print "There are %d users to process" % num_of_users, 
        print ">"*10
        user_jobs.join()
        info_results.join()
        print "+"*10, "user_jobs' length is ", user_jobs.qsize()
        print "+"*10, "info_results' length is ", info_results.qsize()
        write_data_into_file(r)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in runn all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "user_jobs' length is ", user_jobs.qsize()
        print "+"*10, "info_results' length is ", info_results.qsize()
    write_data_into_file(r)


def single_process():
    # load weibo account into redis cache
    r = redis.StrictRedis(**REDIS_SETTING)
    # for weibo in WEIBO_ACCOUNT_LIST:
    auth = '%s--%s' % ('jiangzhibinking@outlook.com', 'jzbwymxjno1_wb')
    cookie = gen_cookie('jiangzhibinking@outlook.com', 'jzbwymxjno1_wb')
    if cookie:
        # import ipdb; ipdb.set_trace()
        r.hset(ACTIVATED_COOKIE, auth, cookie)
    raise Exception('Stop')
    todo=0
    dao = WeiboWriter(OUTER_MYSQL)
    new_users = dao.read_new_user_from_db()
    for user in new_users:  # iterate
        todo += 1
        print user
        all_account = r.hkeys(ACTIVATED_COOKIE)
        if not all_account:  # no any weibo account
            raise Exception('All of your accounts were Freezed')
        auth = pick_rand_ele_from_list(all_account)
        print auth
        account, password = auth.split('--')
        # operate spider
        spider = BozhuInfoSpider(user, account, password)
        spider.read_cookie(r)
        spider.gen_html_source()
        if spider.check_abnormal_status():
            spider.remove_cookie(r)
        else:
            spider.parse_bozhu_info()
        del spider
        if todo > 2:
            break

if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    # single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
