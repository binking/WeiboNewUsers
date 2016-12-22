#-*- encoding:utf-8 -*-
import re
import rsa
import json
import base64
import requests
import binascii

def gen_com_cookie(username, password):
    """登陆新浪微博，获取登陆后的Cookie，返回到变量cookies中"""
    import ipdb; ipdb.set_trace()
    url = 'http://login.sina.com.cn/sso/prelogin.php?entry=sso&callback=sinaSSOController.preloginCallBack&su=%s&rsakt=mod&client=ssologin.js(v1.4.4)%'+username
    html = requests.get(url).content

    servertime = re.findall('"servertime":(.*?),',html,re.S)[0]
    nonce = re.findall('"nonce":"(.*?)"',html,re.S)[0]
    pubkey = re.findall('"pubkey":"(.*?)"',html,re.S)[0]
    rsakv = re.findall('"rsakv":"(.*?)"',html,re.S)[0]

    username = base64.b64encode(username) #加密用户名
    rsaPublickey = int(pubkey, 16)
    key = rsa.PublicKey(rsaPublickey, 65537) #创建公钥
    message = str(servertime) + '\t' + str(nonce) + '\n' + str(password) #拼接明文js加密文件中得到
    passwd = rsa.encrypt(message, key) #加密
    passwd = binascii.b2a_hex(passwd) #将加密信息转换为16进制。

    login_url = 'http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.4)'
    headers = {'User-Agent':'Mozilla/5.0 (X11; Linux i686; rv:8.0) Gecko/20100101 Firefox/8.0'}
    data = {'entry': 'weibo',
        'gateway': '1',
        'from': '',
        'savestate': '7',
        'userticket': '1',
        'ssosimplelogin': '1',
        'vsnf': '1',
        'vsnval': '',
        'su': username,
        'service': 'miniblog',
        'servertime': servertime,
        'nonce': nonce,
        'pwencode': 'rsa2',
        'sp': passwd,
        'encoding': 'UTF-8',
        'prelt': '115',
        'rsakv' : rsakv,
        'url': 'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
        'returntype': 'META'
    }
    import ipdb; ipdb.set_trace()
    html = requests.post(login_url,data=data, headers=headers).content
    if 'retcode=4049' in html:
        print "为了您的帐号安全，请输入验证码"
    if 'retcode=0' in html:
        urlnew = re.findall('location.replace\("(.*?)"',html,re.S)[0]
        #发送get请求并保存cookies
        cookies = requests.get(urlnew).cookies
        return cookies


def get_cn_cookie(account, password):
    """ 获取一个账号的Cookie """
    loginURL = "https://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.15)"
    username = base64.b64encode(account.encode("utf-8")).decode("utf-8")
    postData = {
        "entry": "sso",
        "gateway": "1",
        "from": "null",
        "savestate": "30",
        "useticket": "0",
        "pagerefer": "",
        "vsnf": "1",
        "su": username,
        "service": "sso",
        "sp": password,
        "sr": "1440*900",
        "encoding": "UTF-8",
        "cdult": "3",
        "domain": "sina.com.cn",
        "prelt": "0",
        "returntype": "TEXT",
    }
    session = requests.Session()
    r = session.post(loginURL, data=postData)
    jsonStr = r.content.decode("gbk")
    info = json.loads(jsonStr)
    import ipdb; ipdb.set_trace()
    if info["retcode"] == "0":
        # logger.warning("Get Cookie Success!( Account:%s )" % account)
        print "Get Cookie Success!( Account:%s )" % account
        cookie = session.cookies.get_dict()
        return json.dumps(cookie)
    else:
        # logger.warning("Failed!( Reason:%s )" % info["reason"])
        print "Failed!( Reason:%s )" % info["reason"]
        return ""


def gen_go_cookie(account, password):
    # 编码用户名
    # 把字符串转换bigint
    # 加密密码
    # 获取登录页面的cookie: http://weibo.com/login.php
    # 获取登录参数: http://login.sina.com.cn/sso/prelogin.php?entry=weibo&callback=sinaSSOController.preloginCallBack&su=` + su + `&rsakt=mod&checkpin=1&client=ssologin.js(v1.4.18)&_=
    # 解析json
    # 保存验证码
    # 开始登录: http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)
    # 获取passport并请求: location.replace\('(.*?)'\)

