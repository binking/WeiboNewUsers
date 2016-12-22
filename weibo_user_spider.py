#coding=utf-8
import re
import json
import time
import requests
from datetime import datetime as dt
from bs4 import BeautifulSoup as bs
from template.weibo_utils import catch_parse_error,extract_chinese_info
from template.weibo_spider import WeiboSpider


class WeiboUserSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.info = {}

    @catch_parse_error((AttributeError, Exception))
    def parse_bozhu_info(self):
        res = {}
        # print '4' * 20, 'Parsing Bozhu info'
        if len(self.page) < 20000:
            return res
        # Parse game is on !!!
        cut_code = '\n'.join(self.page.split('\n')[100:])
        elminate_white = re.sub(r'\\r|\\t|\\n', '', cut_code)
        elminate_quote = re.sub(r'\\"', '"', elminate_white)
        short_code = re.sub(r'\\/', '/', elminate_quote)
        
        # The three numbers
        focus_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)
        fans_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?relate=fans.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)
        weibo_num_match = re.search(r'\<a bpfilter="page_frame"  class="t_link S_txt1" href="http://weibo.com/.*?home.*?" \>\<(\w+) class=".+?"\>(\d+)\</(\w+)\>', short_code)

        if focus_num_match and fans_num_match and weibo_num_match:
            self.info['focus_num'] = int(focus_num_match.group(2))
            self.info['fans_num'] = int(fans_num_match.group(2))
            self.info['weibo_num'] = int(weibo_num_match.group(2))
        else:
            return res
        # parse basic info
        info_units = re.findall('\<li class="li_1 clearfix"\>\<\w+ class="pt_title S_txt2"\>(.+?)\</\w+?\>\<\w+? (class|href)=".+?"\>(.*?)\</\w+?\>\</li\>', short_code)
        if not info_units:
            return res
        for unit in info_units:
            attr, _, value = unit
            if '昵称' in attr:
                self.info['nickname'] = value
            elif '真实姓名' in attr:
                self.info['realname'] = value
            elif '所在地' in attr:
                self.info['location'] = value
            elif '性别' in attr:
                self.info['gender'] = value
            elif '性取向' in attr:
                self.info['sex_tendancy'] = value
            elif '感情状况' in attr:
                self.info['emotion'] = value
            elif '生日' in attr:
                self.info['date_of_birth'] = value
            elif '简介' in attr:
                self.info['introduction'] = value
            elif '邮箱' in attr:
                self.info['email'] = value
            elif 'QQ' in attr:
                self.info['qq'] = value
            elif '注册时间' in attr:
                self.info['registration_date'] = value
            elif '博客' in attr:
                self.info['blog_url'] = value
                if 'href' in value and re.search(r'\>(https?://[^\>\<]*?)\<', value):
                    self.info['blog_url'] = re.search(r'\>(https?://[^\>\<]*?)\<', value).group(1)
            elif '个性域名' in attr:
                self.info['domain'] = value
                if 'href' in value and re.search(r'\>(https?://[^\>\<]*?)\<', value):
                    self.info['domain'] = re.search(r'\>(https?://[^\>\<]*?)\<', value).group(1)
            elif '大学' in attr:
                self.info['university'] = extract_chinese_info(value)
            elif '高中' in attr:
                self.info['high_school'] = extract_chinese_info(value)
            elif '标签' in attr:
                self.info['label'] = extract_chinese_info(value)
            elif '公司' in attr:
                self.info['company'] = extract_chinese_info(value)
        # fill other info
        # import ipdb; ipdb.set_trace()
        self.info['uri'] = self.url
        self.info['weibo_user_url'] = '/'.join(self.url.split('/')[:-1])
        return self.info

    @catch_parse_error((Exception,))
    def get_user_label(self):
        """
        curl 'http://m.weibo.cn/container/getIndex?containerid=2302832780282910_-_INFO' -H 'Cookie: _T_WM=ebb1b376770e7c50b163c130f3b613b0; ALF=1483771241; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEE9muOxrNviQ7Hk45YzrEr4fyA07RS0s1LsNgx2OGI3mU.; SUB=_2A251TXD0DeTxGeNG71EX8ybKwj6IHXVWzhC8rDV6PUJbktANLXWikW1aAjteiCcBbz0dFp_hgUr_JAFidg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5o2p5NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=0TPFsv54MksaWM; SSOLoginState=1481179300; M_WEIBOCN_PARAMS=from%3Dfeed%26luicode%3D10000011%26lfid%3D2302832780282910_-_INFO%26fid%3D2302832780282910_-_INFO%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2302832780282910_-_INFO' -H 'X-Requested-With: XMLHttpRequest' -H 'Proxy-Connection: keep-alive' --compressed
        """
        label = ''
        # import ipdb; ipdb.set_trace()
        if len(self.page) < 100:
            return label
        user_info_dict = json.loads(self.page)
        # print user_info_dict['cards'][0]['card_group']
        for card in user_info_dict['cards'][0]['card_group']:
            if card.get('item_name') and '标签' in card['item_name']:
                return card['item_content']
        return label
