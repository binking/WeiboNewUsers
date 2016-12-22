#!/usr/bin/env python
# -*- coding:utf-8 -*-
from spider import AsySpider

class TestWeiboAsyncSpider(AsySpider):
    def handle_html(self, url, html):
        