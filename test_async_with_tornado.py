#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
from lxml import etree
from datetime import timedelta  
from tornado import httpclient, gen, ioloop, queues


class AsySpider(object):  
    """A simple class of asynchronous spider."""
    def __init__(self, urls, concurrency=10, results=None, **kwargs):
        urls.reverse()
        self.urls = urls
        self.concurrency = concurrency
        self._q = queues.Queue()
        self._fetching = set()
        self._fetched = set()
        if results is None:
            self.results = []

    def fetch(self, url, **kwargs):
        fetch = getattr(httpclient.AsyncHTTPClient(), 'fetch')
        return fetch(url, raise_error=False, **kwargs)

    def handle_html(self, url, html):
        """handle html page"""
        # print(url)
        pass

    def handle_response(self, url, response):
        """inherit and rewrite this method if necessary"""
        if response.code == 200:
            self.handle_html(url, response.body)

        elif response.code == 599:    # retry
            self._fetching.remove(url)
            self._q.put(url)

    @gen.coroutine
    def get_page(self, url):
        try:
            response = yield self.fetch(url)
            #print('######fetched %s' % url)
        except Exception as e:
            print('Exception: %s %s' % (e, url))
            raise gen.Return(e)
        raise gen.Return(response)

    @gen.coroutine
    def _run(self):
        @gen.coroutine
        def fetch_url():
            current_url = yield self._q.get()
            try:
                if current_url in self._fetching:
                    return

                #print('fetching****** %s' % current_url)
                self._fetching.add(current_url)

                response = yield self.get_page(current_url)
                self.handle_response(current_url, response)    # handle reponse

                self._fetched.add(current_url)

                for i in range(self.concurrency):
                    if self.urls:
                        yield self._q.put(self.urls.pop())

            finally:
                self._q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()

        self._q.put(self.urls.pop())    # add first url

        # Start workers, then wait for the work queue to be empty.
        for _ in range(self.concurrency):
            worker()

        yield self._q.join(timeout=timedelta(seconds=300000))
        try:
            assert self._fetching == self._fetched
        except AssertionError:
            print(self._fetching-self._fetched)
            print(self._fetched-self._fetching)

    def run(self):
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self._run)


class Jiba51Spider(AsySpider):  
    def handle_html(self, url, html):
        _id = url.rsplit('/', 1)[1].split('.')[0]
        print _id, type(html), len(html)
        # return 
        # filename = './html/' + _id + '.html'
        # with open(filename, 'w+') as f:
        #     f.write(html)


def main():  
    urls = []
    for page in range(1, 80000):
        urls.append('http://www.jb51.net/article/%s.htm' % page)
    s = Jiba51Spider(urls)
    s.run()


if __name__ == '__main__':  
    main()