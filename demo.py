#!/usr/bin/env python
#coding=utf8

__author__ = 'Ricky'

import urlparse
import BeautifulSoup as bs
import requests
import sqlite3

url = 'http://nba.hupu.com/'

html = requests.get(url)

print u'状态代码'+str(html.status_code)

#def findUrl(url):
#    sql  = 'SELECT * FROM `url` WHERE `url`="{0}"'.format(url)
#    conn = sqlite3.connect('/www/spider/spider.db')
#    cu   = conn.cursor()
#    cu.execute(sql)
#    return cu.fetchone()
#
#print findUrl('http://nba.hupu.com/s')