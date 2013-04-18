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
q = Queue.Queue(10000)
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
        content = requests.get(url)
        if content.status_code == 200:
            self._encoding = content.encoding
            return content.text
        else:
            print url+u'返回代码:'+str(content.status_code)
            return False

    def handleHtml(self, url, html):
        '''
        分析HTML文件中的A标签
        '''
        soup = bs.BeautifulSoup(html)
        keyword = soup.findAll(attrs={'name': 'keyword'})

        if not keyword:
            keyword = soup.findAll(attrs={'name': 'keywords'})

        if not keyword:
            keyword = soup.findAll(attrs={'name': 'Keyword'})

        if keyword and keyword[0] and keyword[0]['content']:
            self._keyword = keyword[0]['content']
        else:
            self._keyword = ''

        a = soup.findAll('a')
        tmp = list()
        for x in a:
            try:
                if x['href']:
                    tmp.append(x['href'])
            except Exception as e:
                pass

        for x in tmp:
            self.handleAtags(x)

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
        sql  = 'INSERT INTO `url` (`url`, `keyword`) VALUES("{0}", "{1}")'.format(url, keyword.encode(self._encoding))
        conn = sqlite3.connect('/www/spider/spider.db')
        cu   = conn.cursor()
        cu.execute(sql)
        conn.commit()
        conn.close()
        return True

    def findUrl(self, url):
        url_d = url+'/'
        sql  = 'SELECT * FROM `url` WHERE `url`="{0}" OR `url`="{1}"'.format(url.encode(self._encoding), url_d.encode(self._encoding))
        conn = sqlite3.connect('/www/spider/spider.db')
        cu   = conn.cursor()
        cu.execute(sql)

        data = cu.fetchone()
        conn.close()
        return data


for i in range(int(result.thread_num)):
    Spider(q, result,).start()
    time.sleep(5)
