#coding=utf-8
import re
from weibo_user_spider import WeiboUserSpider
from weibo_user_writer import WeiboUserWriter
from zc_spider.weibo_config import QCLOUD_MYSQL

curl = """curl 'http://weibo.com/1943986154/info' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Referer: http://login.sina.com.cn/sso/login.php?url=http%3A%2F%2Fweibo.com%2F1943986154%2Finfo&_rand=1489670258.2137&gateway=1&service=miniblog&entry=miniblog&useticket=1&returntype=META&_client_version=0.6.23' -H 'Cookie: SINAGLOBAL=2129770368437.427.1472128914307; wb_g_upvideo_5888169063=1; un=18813105413; _s_tentry=baike.baidu.com; Apache=3478976933205.107.1489236462465; ULV=1489236462516:8:1:1:3478976933205.107.1489236462465:1487430529743; YF-Page-G0=c6cf9d248b30287d0e884a20bac2c5ff; YF-V5-G0=e6f12d86f222067e0079d729f0a701bc; SSOLoginState=1489326534; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094; wvr=6; SCF=Ao6xZYojEyI0bUcRtMyFuyf9WckLYDJM0kzvZNMSOQBeoMjtkEyRwUqDUoYZ46_Sn3HolB3mq5uuDWflAXUynBo.; SUB=_2A251zuAjDeRxGeVG7FYT8i_OzzWIHXVWulbrrDV8PUNbmtBeLW3CkW8Apjeq-xrzL73PvNnGWJZqtVPIiA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhxM.AD9EjGmQSc51FnJvMU5JpX5KMhUgL.FoeRS0BEeo2ESh.2dJLoIEBLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=09P9Pt3GKlCs1e; ALF=1521206258; UOR=cartoon.tudou.com,widget.weibo.com,login.sina.com.cn' -H 'Connection: keep-alive' -H 'Cache-Control: max-age=0' --compressed"""
spider = WeiboUserSpider('http://weibo.com/5943091854/info', 'binking', '', timeout=20)

spider.use_cookie_from_curl(curl)
status = spider.gen_html_source()
res = spider.parse_bozhu_info()
for k, v in res.items():
    print k, v
import ipdb ;ipdb.set_trace()
dao = WeiboUserWriter(QCLOUD_MYSQL)
dao.insert_new_user_into_db(res)
