#coding=utf-8
from config.weibo_config import *
from dao.weibo_writer import WeiboWriter
from weibo_bozhu_info_spider import BozhuInfoSpider

if __name__=='__main__':
    urls = ['http://weibo.com/p/1005052008064375/info']
    for url in urls:
        bis = BozhuInfoSpider(url, 'test_user', 'test_passwd')
        bis.use_test_cookie()
        bis.add_request_header()
        bis.gen_html_source()
        info = bis.parse_bozhu_info()
        db =  WeiboWriter(QCLOUD_MYSQL)
        db.insert_new_user_into_db(info)
        print '*'*30
    # print REDIS_KEY
    # print '2', ABUYUN_USER
    # import ipdb; ipdb.set_trace()
    # 
    # for url in db.read_co_url_from_db():
    #    print url

    