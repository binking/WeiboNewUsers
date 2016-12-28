#-*- coding: utf-8 -*-
import os
from weibo_config import WEIBO_MANUAL_COOKIES

if __name__=="__main__":
    for key in WEIBO_MANUAL_COOKIES:
        html = os.popen(WEIBO_MANUAL_COOKIES[key] + ' --silent').read()
        if len(html) < 20000:
            print key, "已失效"
        else:
            print key, "还活着"
