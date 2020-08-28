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
historyList = []
def main(analyDate):
    print analyDate
    # url = 'http://live.leisu.com/wanchang?date=2017/04/12'
    url = 'http://live.leisu.com/wanchang?date=' + analyDate
    target = requests.get(url)
    source = BeautifulSoup(target.text.encode("utf-8"), "lxml")

    table = source.find('table', {'class': 'main-content'})

    thead = table.find('thead')
#    date = thead.find('span').text

    tbody = table.find('tbody')
    race_list = tbody.findAll('tr')
    #print 'game have: ' + str(len(race_list))
    
    count = 1

    result_dir = 'Data/History/' + analyDate
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    for tr in race_list:
        race_id = tr.get('data-id')
        if tr.find('td', {'class': 'tr-time'}):
            rtime = tr.find('td', {'class': 'tr-time'}).text
        team_a = tr.get('data-away')
        team_b = tr.get('data-home')
        GameName = tr.get('data-matchevent')
        if tr.find('td',{'class':'tr-round'}):
    	   tround = tr.find('td',{'class':'tr-round'}).text
    	temp = team_a.split('$')
    	team_a = temp[0]
    	temp = team_b.split('$')
    	team_b = temp[0]
    	temp = GameName.split('$')
    	GameName = temp[0]

        score = tr.find('td', {'class': 'tr-score tr-vs-score'}).text.split('-')
        team_a_score = score[0]
        team_b_score = score[1]
        if tr.find('td', {'class': 'tr-half'}):
            half_score = tr.find('td', {'class': 'tr-half'}).text.split('-')
        
        if tr.find('td',{'class':'tr-corner'}):
            corner_score = tr.find('td',{'class':'tr-corner'}).text.split('-')
        team_a_half_score = half_score[0]
        team_b_half_score = half_score[1]
    	inputTime = analyDate+' '+rtime
    	inputTime = datetime.datetime.strptime(inputTime, '%Y-%m-%d %H:%M')
        timestamp = time.mktime(inputTime.timetuple())
        if len(corner_score)>1:
            team_a_corner = corner_score[0]
            team_b_corner = corner_score[1]
        else :
            team_a_corner = ''
            team_b_corner = ''
        race = {
            'Date':int(timestamp),
            'round':tround,
            'League':GameName,
            'MatchID': race_id,
            'Home': {
            'Name': team_a,
            'Score':{
            'Total': team_a_score,
            'Half':team_a_half_score,
            'Corner':team_a_corner
            }
            },
            'Away': {
                'Name': team_b,
                'Score':{
                'Total': team_b_score,
                'Half':team_b_half_score,
                'Corner':team_b_corner
		          }
            }
        }
        historyList.append(race)
        # race_file = open(result_dir + '/' + race_id + '.json', 'w+')
        # race_file.write(json.dumps(race, indent=4, sort_keys=True, ensure_ascii=False))
        # race_file.close()

if __name__ == "__main__":
    begin_date = datetime.datetime.strptime("2004-1-3", "%Y-%m-%d") 
    end_date = datetime.datetime.strptime("2017-6-9", "%Y-%m-%d")
    dateList=[]
   
    while(begin_date < end_date):	
        #print begin_date.strftime("%Y-%m-%d")
        dateList.append(begin_date.strftime("%Y-%m-%d"))
	#main(begin_date.strftime("%Y-%m-%d"))
        begin_date = begin_date + datetime.timedelta(days=1)
        #print dateList
    print len(dateList)
    print('Crawling article in multi-processing...')
    target_pool = Pool(70)
    target_pool.map(main, dateList)
    target_pool.close()
    print('Crawling is done.')
    from pymongo import MongoClient
    client = MongoClient()
    db = client['crawler']
    coll = db['history']
    bulk = db.items.initializeUnorderedBulkOp()
    bulk.insert(historyList)
    bulk.execute()


