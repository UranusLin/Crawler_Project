# -*- coding: UTF-8 -*-
import requests
from bs4 import BeautifulSoup
import re
import json
import os
import datetime
import sys
import csv
import codecs
import datetime
import math
import time
from multiprocessing import Pool
reload(sys)
sys.setdefaultencoding('utf-8')

report_list = []
pages_list = []

def main(url):
    #第一次取得第一頁和第一頁的url
    crawlerReport(url,'1')
    print len(report_list)
    #print len(pages_list)
    for curl in pages_list :
        crawlerReport(curl,'0')
        print len(report_list)

    #開始爬取場次資料
    for uurl in report_list :


def crawlerReport (url,count):
    #count 為 第一次爬曲分頁資料後就不再爬取分頁資料
    target = requests.get(url)
    source = BeautifulSoup(target.text.encode("utf-8"), "lxml")

    href = source.find_all('a', href=True)
    white = source.find_all('td', {'class': 'white'})
    owner_count = 0 
    if count == '1' :   
        for hr in href :
            reports = 'reports/'
            pages = '?page='
            title = hr['href']
            if title.find(reports) > 0 :
                if white[owner_count].text.find('stevenjob') == 0 :
                    report_list.append(title)
                owner_count = owner_count +1
            elif title.find(pages) > 0 :
                #print hr.text
                if hr.text.find('Next') < 0 :
                    pages_list.append(title)
    elif count == '0' :
        for hr in href :
            reports = 'reports/'
            pages = '?page='
            title = hr['href']
            if title.find(reports) > 0 :
                #print title
                if white[owner_count].text.find('stevenjob') == 0 :
                    report_list.append(title)
                
#def crawlerUser (url):

if __name__ == "__main__":
    url = "https://www.warcraftlogs.com/guilds/reportslist/33194/"
    main(url)

target = requests.get("https://www.warcraftlogs.com/reports/zAFhpt1P26VWwKB4")
table = BeautifulSoup(target.text.encode("utf-8"), "lxml").find_all('a', href = '#')

