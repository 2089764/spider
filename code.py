#!/usr/bin/env python
#coding=utf8

__author__ = 'Ricky'
__version__ = '0.1v'

import threading
import argparse
import logging
import urlparse
import Queue
import sqlite3
import time
import requests
import BeautifulSoup as bs



parse = argparse.ArgumentParser(description='Python爬虫程序 --Ricky Liu')
parse.add_argument('-u', action='store', dest='start', help='指定爬虫开始地址', default='http://robinfai.com')
parse.add_argument('-d', action='store', dest='deep', help='指定爬虫深度', default=10)
parse.add_argument('-t', action='store', dest='thread_num', help='线程数', default=10)
parse.add_argument('-l', action='store', dest='level', help='日志记录级别', default=5)
parse.add_argument('-db', action='store', dest='db', help='数据库文件', default='spider')
parse.add_argument('-testself', action='store', dest='testself', help='程序自测', default=True)

result = parse.parse_args()
q = Queue.Queue(1000)
q.put(result.start)

class Spider(threading.Thread):
    def __init__(self, q, result):
        self._q = q
        self._deep = result.deep
        self._db = result.db
        threading.Thread.__init__(self)

    def run(self):
        while self._q.qsize() > 0:
            self._url = self._q.get()

            searchResult = self.findUrl(self._url)

            if searchResult is not None:
                continue

            self._content = self.get_content_by_url(self._url)
            if not self._content:
                continue

            self.handleHtml(self._url, self._content)
            self.save_url(self._url, self._keyword)
            print self._url + u'采集完成' + u'还有'+str(self._q.qsize())+u'条记录需求采集'

        print u'进程'+threading.currentThread().getName()+u'采集完毕'

    def get_content_by_url(self, url):
        conent = requests.get(url)
        if conent.status_code == 200:
            return conent.text
        else:
            print url+u'返回代码:'+str(conent.status_code)
            return False

    def handleHtml(self, url, html):
        '''
        分析HTML文件中的A标签
        '''
        soup = bs.BeautifulSoup(html)
        keyword = soup.findAll(attr={'name': 'keyword'})
        if keyword and keyword[0]:
            self._keyword = keyword[0]['content']
        else:
            self._keyword = ''

        a = soup.findAll('a')
        for x in a:
            try:
                self.handleAtags(x['href'])
            except KeyError:
                pass

    def handleAtags(self, url):
        parse = urlparse.urlparse(url)
        home = urlparse.urlparse(self._url)

        netloc = parse.netloc
        scheme = parse.scheme

        if netloc == '':
            netloc = home.netloc

        if scheme == '':
            scheme = home.scheme

        newUrl = urlparse.urlunparse((scheme, netloc, parse.path, parse.params, parse.query, parse.fragment))
        newParse = urlparse.urlparse(newUrl)

        if newUrl and newParse.hostname == home.hostname and (parse.scheme == 'http' or parse.scheme == 'https' or parse.scheme == ''):
            if self.findUrl(newUrl) is not None:
                print newUrl+u'已存在'
            else:
                self._q.put(newUrl)
        else:
            return False

    def save_data(self, ):
        '''
        保存用户输入的关键词相关的页面
        '''
        pass

    def save_url(self, url, keyword):
        '''
        保存已爬过的URL
        '''
        sql  = 'INSERT INTO `url` (`url`, `keyword`) VALUES("{0}", "{1}")'.format(url, keyword)
        conn = sqlite3.connect('/www/spider/spider.db')
        cu   = conn.cursor()
        cu.execute(sql)
        conn.commit()
        conn.close()
        return True

    def findUrl(self, url):
        sql  = 'SELECT * FROM `url` WHERE `url`="{0}"'.format(url)
        conn = sqlite3.connect('/www/spider/spider.db')
        cu   = conn.cursor()
        cu.execute(sql)

        data = cu.fetchone()
        conn.close()
        return data


for i in range(int(result.thread_num)):
    Spider(q, result,).start()
    time.sleep(5)


'''
import thread, time, random

count = 0

def threadTest():
    global count
    for i in xrange(100):
        print 'id-'+str(thread.get_ident())+':' + str(count)
        count += 1

for i in range(10):
    thread.start_new_thread(threadTest, ())

time.sleep(1)
print count
'''

'''
import argparse


parser = argparse.ArgumentParser(description='爬虫程序')

parser.add_argument('-u', action='store', help='指定爬虫开始地址', dest='url', default='http://www.sina.com/')
parser.add_argument('-d', action='store', default=10, help='指定爬虫深度', dest='deep')
parser.add_argument('-thread', action='append', default=10, help='指定线程池大小，多线程爬取页面，可选参数, 默认10', dest='thread_num', type=int)
parser.add_argument('-dbfile', action='append', default='dbfile', help='存放结果到指定数据库(sqlite)文件中', dest='dbfile',)
parser.add_argument('-key', action='append', default='all', help='页面内的关键词，获取满足该关键词的网页, 可选参数, 默认为所有页面', dest='key',)
parser.add_argument('-l', action='append', default='1', help='日志记录文件记录详细程度，数字越大记录越详细, 可选参数，默认spider.log', dest='level',)
parser.add_argument('-testself', action='append', help='程序自测，可选参数', dest='testself')

#print parser.parse_args()

result = parser.parse_args()

print result.url
'''

'''
import sqlite3

conn = sqlite3.connect('/www/spider/spider.db')
cu = conn.cursor()

sql = 'SELECT * FROM `user`'
cu.execute(sql)

print cu.fetchone()


cu.close()
'''

'''
import requests
import BeautifulSoup as bs

url = 'http://www.douban.com/'

html = requests.get(url).text

soup = bs.BeautifulSoup(html)

result = soup.findAll('a')

arr = []

for x in result:
    arr.append(x['href'])

arr = {}.fromkeys(arr).keys()

for x in arr:
    print x
'''

'''
import Queue
import threading
import time
import random

q = Queue.Queue(0)
NUM_WORKERS = 3

class MyThread(threading.Thread):
    def __init__(self, input, worktype):
        self._jobq = input
        self._work_type = worktype
        threading.Thread.__init__(self)

    def run(self):
        while True:
            if self._jobq.qsize() > 0:
                job = self._jobq.get()
                worktype = self._work_type
                self._process_job(job, worktype)
            else:
                break

    def _process_job(self, job, worktype):
        dojob(job)


def dojob(job):
    time.sleep(random.random() * 3)
    print 'doing ', job

if __name__ == '__main__':
    print 'begin...'
    for i in range(NUM_WORKERS * 2):
        q.put(i)

    print "job q'size", q.qsize()

    for x in range(NUM_WORKERS):
        MyThread(q, x).start()
'''
'''
import Queue

q = Queue.Queue(10)
for x in range(10):
    q.put(x)

print "q'size is ", q.qsize()

while True:
    if q.qsize() > 0:
        print q.get()
    else:
        break
'''