#!/usr/bin/env python
# -*- encoding:utf-8 -*-
# A tiny Website Spider
# Created by Noble in EM at 2016.12.19

import threading
import urllib
import Queue
import urlparse
import requests
import time
import traceback
import os
import sys

from bs4 import BeautifulSoup
from simhash import Simhash

reload(sys)
sys.setdefaultencoding("utf-8")


class Spider():

    def __init__(self, url, keyword):
        self.Chars = [',', '-', '_']
        self.keyword = keyword
        self.url = self.url_init(url)
#		print self.url
        self.scheme, self.host = self.url_parse(self.url)
        self.links = set()
        self.url_queue = Queue.Queue()
        self.url_queue.put(self.url)
        self.urls_in_queue = []
        self.lock = threading.Lock()
        self.crawled = set()
        self.queue = set()
        self.Etlurls = []
        self.file = open("%s.txt" % self.host, 'w')

    def url_init(self, url):
        try:
            u = urlparse.urlparse(url)
            if not u.netloc:
                u = urlparse.urlparse("http://" + url, 'http')
                if not u.netloc:
                    u = urlparse.urlparse("https://" + url)
                    if not u.loc:
                        return ""
                        print "not valid url"

                return urlparse.urlunparse(u)
            else:
                return url
        except Exception, e:
            pass
            print "not valid url"
            print e

    def url_etl(self, url):
        params_new = {}
        u = urlparse.urlparse(url)
        query = urllib.unquote(u.query)
        if not query:
            return url
        path = urllib.unquote(u.path)
        params = urlparse.parse_qsl(query, True)
        for k, v in params:
            if v:
                params_new[k] = self.etl(v)
        query_new = urllib.urlencode(params_new)
        url_new = urlparse.urlunparse(
            (u.scheme, u.netloc, u.path, u.params, query_new, u.fragment))
        # print url_new
        return url_new

    def etl(self, str):
        chars = ""
        for c in str:
            c = c.lower()
            if ord('a') <= ord(c) <= ord('z'):
                chars += 'A'
            elif ord('0') <= ord(c) <= ord('9'):
                chars += 'N'
            elif c in self.Chars:
                chars += 'T'
            else:
                chars += 'C'
        return chars

    def url_rule(self, url):
        if url.find(self.keyword) > -1:
            if url.startswith('http://') or url.startswith("https://"):
                if url.find('#') > 0:
                    url = url[:url.find('#')]
                if url.endswith(('.jpg', '.zip', '.gif', '.png', '.pdf', '.html', '.shtml')):
                    return ''
                for link in self.Etlurls:
                    same = self.url_compare(self.url_etl(url), link)
                    if same:
                        return ''
                    else:
                        continue
                return url
            else:
                return ''
        else:
            return ""

    def url_compare(self, url, link):
        dis = Simhash(url).distance(Simhash(link))
        if -1 < dis < 4:
            return True
        else:
            return False

    def url_parse(self, url):
        u = urlparse.urlparse(url)
        return u.scheme, u.netloc

    def enqueue(self, url):
        if url in self.urls_in_queue:
            return False
        else:
            self.urls_in_queue.append(url)
            self.url_queue.put(url)

    def http_request(self, url, timeout=30):
        try:
            if not url:
                return ''
            resp = requests.get(url=url, timeout=timeout, headers={
                                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36'})
            resp_headers = dict(resp.headers)
            status = resp.status_code
            if resp_headers.get('Content-Type', '').find('text') >= 0 or resp_headers.get('Content-Type', '').find('html') >= 0 or \
                    int(resp_headers.get('content-length', '0')) <= 1048576:
                html_doc = resp.content
            else:
                html_doc = ''
#			if status in [200, 302]:
            # print html_doc
            return html_doc
        except Exception, e:
            print e
            return ''

    def spider(self, url_to_spider):
        #url = url_queue.get()
        try:
            _url = url_to_spider
            print "[Start Spider:]" + _url
            if not _url:
                return
            html_doc = self.http_request(_url)
            # print html_doc

            if html_doc:
                soup = BeautifulSoup(html_doc, 'html.parser')
                links = soup.find_all('a')
                for l in links:
                    url = l.get('href')
                    if url:
                        url = self.url_rule(url)
                        if url:
                            self.lock.acquire()
                            self.enqueue(url)
                            self.Etlurls.append(self.url_etl(url))
                            self.crawled.add(url)
                            self.lock.release()
                            self.file.write(url + "\r\n")
                            print url
                        else:
                            # print "invalid url"
                            continue
            else:
                print "Failed to get target html"
        except KeyboardInterrupt, e:
            print "User aborted! Goodbye!"
            sys.exit(1)

    def work(self):
        url_to_spider = ''
        while True:
            try:
                url_to_spider = self.url_queue.get(timeout=1.0)
                # print url_to_spider
            except Exception, e:
                print e
            #	return
            try:
                self.spider(url_to_spider)
                # time.sleep(10)
                # self.url_queue.task_done()
                if self.url_queue.empty():
                    print "No more links found! Please input ctrl-c to quit!"
                print "spider finish"
            except Exception, e:
                print "Error: ", traceback.format_exc()
                pass

    def run(self, threads=10):
        threads_list = []
        # print "Spider Start at %s" %time.ctime()
        for i in range(threads):
            t = threading.Thread(target=self.work)
            threads_list.append(t)
            t.setDaemon(True)
            t.start()

        while True:
            try:
                time.sleep(1.0)
                if len(self.crawled) > 10000:
                    print "100000 links has Got!"
                    sys.exit(0)
            except KeyboardInterrupt, e:
                print "User aborted! Please wait all threads to stop!"
                print "Spider stop at %s" % time.ctime()
                sys.stdout.flush()
                sys.exit(0)
        for t in threads_list:
            t.join()

        self.file.close()
        print "Spider Finish at %s" % time.ctime()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        print "Usage: python Spider.py host"
        sys.exit(0)
    url = sys.argv[1]
    keyword = sys.argv[2]
    print "Spider start at %s" % time.ctime()

    spider = Spider(url, keyword)
    spider.run()
