# -*- coding: UTF-8 -*-
from PIL import Image, ImageDraw
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
reload(sys)
sys.setdefaultencoding('utf-8')
title = ['賽事','比賽日期','主隊','客隊','主隊總分','客隊總分','主隊半場','客隊半場','','賽事','比賽日期','主隊','客隊','主隊總分','客隊總分','主隊半場','客隊半場']
begin_date = datetime.datetime.strptime("2011/1/1", "%Y/%m/%d") 
dot6 = ['0.6','0.6','0.6','0.6','0.6','0.6']
dot4 = ['0.4','0.4','0.4','0.4','0.4','0.4']
doth6 = ['0.6','0.6','0.6','0.6','0.6']
doth4 = ['0.4','0.4','0.4','0.4','0.4']

sctitle = ['主隊全場機率(%):','0分','1分','2分','3分','4分','>5分','','','主隊全場機率(%):','0分','1分','2分','3分','4分','>5分']
sctitle1 = ['客隊0分','客隊1分','客隊2分','客隊3分','客隊4分','客隊>5分']
sctitle2 = ['主隊半場機率(%):','0分','1分','2分','3分','>4分','','','','主隊半場機率(%):','0分','1分','2分','3分','>4分' ]

sort_all = []
sort_3 = []
sort_all_h = []
sort_3_h = []
sort_all_d = []
sort_3_d = []
sort_all_h_d = []
sort_3_h_d = []


def countrate(rate):
    temp = 0
    for a in rate:
        if int(a) > 4 :
            temp = temp +1 
    countRate = [rate.count('0')/float(len(rate)),rate.count('1')/float(len(rate)),rate.count('2')/float(len(rate)),rate.count('3')/float(len(rate)),rate.count('4')/float(len(rate)),temp/float(len(rate))]
    return countRate

def countrateHalf(rate):
    temp = 0
    for a in rate:
        if int(a) > 3 :
            temp = temp +1 
    countRate = [rate.count('0')/float(len(rate)),rate.count('1')/float(len(rate)),rate.count('2')/float(len(rate)),rate.count('3')/float(len(rate)),temp/float(len(rate))]
    return countRate

def nowHomeAll(rate):
    homeAll = [rate[0] ,rate[1]+(rate[2]*0.1) ,(rate[2]*0.7)+(rate[3]*0.2) ,(rate[3]*0.6)+(rate[4]*0.3)+(rate[2]*0.2) ,(rate[4]*0.6)+(rate[5]*0.5)+(rate[3]*0.2) ,(rate[5]*0.5)]
    return homeAll

def nowHomeHalf(rate):
    homeHalf = [rate[0] ,rate[1]+(rate[2]*0.2) ,(rate[2]*0.7)+(rate[3]*0.5) ,(rate[3]*0.5)+(rate[4]*0.7)+(rate[2]*0.1) ,(rate[4]*0.3)]
    return homeHalf

def nowAwayAll(rate):
    awayAll = [rate[0] ,rate[1]+(rate[2]*0.2) ,(rate[2]*0.6)+(rate[3]*0.3) ,(rate[3]*0.6)+(rate[4]*0.5)+(rate[2]*0.2) ,(rate[4]*0.5)+(rate[5]*0.7)+(rate[3]*0.1) ,(rate[5]*0.3)]
    return awayAll

def nowAwayHalf(rate):
    awayHalf = [rate[0] ,rate[1]+(rate[2]*0.3) ,(rate[2]*0.5)+(rate[3]*0.5) ,(rate[3]*0.5)+(rate[4]*0.7)+(rate[2]*0.2) ,(rate[4]*0.3)]
    return awayHalf

def getRecent(analysis_id,ndate,home,away,GameName):

    url = 'http://live.leisu.com/shujufenxi-' + analysis_id
    # url = 'http://live.leisu.com/shujufenxi-2026228'
    target = requests.get(url)
    source = BeautifulSoup(target.text.encode("utf-8"), "lxml")
    info = source.find('div',{'class':'history-info'}).text
    recent_table = source.find('table', {'class': 'recent-rival'})
    recent_battle_list = recent_table.find('tbody').findAll('tr')
    result_dir = 'Result/Analysis/' + ndate + '/' + GameName
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    funxi = open(result_dir + '/' + home + '-' + away + '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    w = csv.writer(funxi)
    w.writerow(info)
    w.writerow(title)
    if len(recent_battle_list) > 1:   
        away_data = []
        home_data = []
        date_home_data = []
        date_away_data = []

        # recent_battle_file = open(result_dir + 'shujunxi' + '.json', 'w+')
        # ------主場相符--------
        # 主主
        H_Home = []
        H_Home_half = []
        date_H_Home = []
        date_H_Home_half = []
        # 客客
        A_Away = []
        A_Away_half = []
        date_A_Away = []
        date_A_Away_half = []
        #--------主場不符------- 
        # 主客
        H_Away = []
        H_Away_half = []
        date_H_Away = []
        date_H_Away_half = []
        # 客主
        A_Home = []
        A_Home_half = []
        date_A_Home = []
        date_A_Home_half = []
        for record in recent_battle_list:
            H_Home_m_all = []
            A_Away_m_all = []
            H_Away_m_all = []
            A_Home_m_all = []
            H_Home_m_allHalf = []
            A_Away_m_allHalf = []
            H_Away_m_allHalf = []
            A_Home_m_allHalf = []

            date_H_Home_m_all = []
            date_A_Away_m_all = []
            date_H_Away_m_all = []
            date_A_Home_m_all = []
            date_H_Home_m_allHalf = []
            date_A_Away_m_allHalf = []
            date_H_Away_m_allHalf = []
            date_A_Home_m_allHalf = []

            H_All = []
            H_Half = []
            A_All = []
            A_Half = []

            date_H_All = []
            date_H_Half = []
            date_A_All = []
            date_A_Half = []

            allRate = []
            allRateHalf = []

            date_allRate = []
            date_allRateHalf = []

            gameName = record.find('div',{'class':'border-name'}).text
            date = record.find('td', {'class': 'time border'}).text
            team_host = record.find('td', {'class': 'vsl'}).text
            team_guest = record.find('td', {'class': 'vsr'}).text
            result = record.find('td', {'class': 'result'}).text
            half_border = record.find('td',{'class': 'half'}).text.split(':')
            # 平手
            if result == '平':
                temp = record.find('td',{'class':'score'}).find('span',{'class':''}).text
                Result_Host = temp
                Result_Guest = temp
            # 勝
            elif result == '胜':
                Result_Host = record.find('td',{'class':'score'}).find('span',{'class':'win'}).text
                Result_Guest = record.find('td',{'class':'score'}).find('span',{'class':''}).text
            # 負
            elif result == '负':
                Result_Host = record.find('td',{'class':'score'}).find('span',{'class':''}).text
                Result_Guest = record.find('td',{'class':'score'}).find('span',{'class':'win'}).text
            # recent_battle_record = {
            #     'AgameName': gameName,
            #     'Date': date,
            #     'Host': team_host,
            #     'Guest': team_guest,
            #     'Result': result,
            #     'Result Host' : Result_Host,
            #     'Result Guest' : Result_Guest,
            #     'Result Host Half':half_border[0],
            #     'Result Guest Half':half_border[1],
            #     'Result info':info[1]
            # }
            # 做日期判斷
            date.replace('/','-')
            racedate = datetime.datetime.strptime(date, "%Y-%m-%d")
            if home == team_host:
                if racedate > begin_date :
                    date_home_data.append([gameName,date,team_host,team_guest,Result_Host,Result_Guest,half_border[0],half_border[1]])
                    date_H_Home.append(Result_Host.encode('ascii', 'ignore'))
                    date_H_Home_half.append(half_border[0].encode('ascii', 'ignore'))
                    date_A_Away.append(Result_Guest.encode('ascii', 'ignore'))
                    date_A_Away_half.append(half_border[1].encode('ascii', 'ignore'))
                home_data.append([gameName,date,team_host,team_guest,Result_Host,Result_Guest,half_border[0],half_border[1]])
                H_Home.append(Result_Host.encode('ascii', 'ignore'))
                H_Home_half.append(half_border[0].encode('ascii', 'ignore'))
                A_Away.append(Result_Guest.encode('ascii', 'ignore'))
                A_Away_half.append(half_border[1].encode('ascii', 'ignore'))

            else :
                if racedate > begin_date :
                    date_away_data.append([gameName,date,team_host,team_guest,Result_Host,Result_Guest,half_border[0],half_border[1]])
                    date_A_Home.append(Result_Host.encode('ascii', 'ignore'))
                    date_A_Home_half.append(half_border[0].encode('ascii', 'ignore'))
                    date_H_Away.append(Result_Guest.encode('ascii', 'ignore'))
                    date_H_Away_half.append(half_border[1].encode('ascii', 'ignore'))
                away_data.append([gameName,date,team_host,team_guest,Result_Host,Result_Guest,half_border[0],half_border[1]])
                A_Home.append(Result_Host.encode('ascii', 'ignore'))
                A_Home_half.append(half_border[0].encode('ascii', 'ignore'))
                H_Away.append(Result_Guest.encode('ascii', 'ignore'))
                H_Away_half.append(half_border[1].encode('ascii', 'ignore'))

            # w.writerow(data)
            # recent_battle_file.write(json.dumps(recent_battle_record, indent=4, sort_keys=True, ensure_ascii=False))
        

        # recent_battle_file.close()
        if len(H_Home)>0:
            H_Home_m = countrate(H_Home)
            H_Home_m_half = countrateHalf(H_Home_half)
            A_Away_m = countrate(A_Away) 
            A_Away_m_half = countrateHalf(A_Away_half)

            H_Home_m_all = nowHomeAll(H_Home_m)
            H_Home_m_allHalf = nowHomeHalf(H_Home_m_half)
            A_Away_m_all = nowAwayAll(A_Away_m)
            A_Away_m_allHalf = nowAwayHalf(A_Away_m_half)

        if len(date_H_Home)>0:
            date_H_Home_m = countrate(date_H_Home)
            date_H_Home_m_half = countrateHalf(date_H_Home_half)
            date_A_Away_m = countrate(date_A_Away) 
            date_A_Away_m_half = countrateHalf(date_A_Away_half)

            date_H_Home_m_all = nowHomeAll(date_H_Home_m)
            date_H_Home_m_allHalf = nowHomeHalf(date_H_Home_m_half)
            date_A_Away_m_all = nowAwayAll(date_A_Away_m)
            date_A_Away_m_allHalf = nowAwayHalf(date_A_Away_m_half)

        if len(H_Away)>0:
            H_Away_m = countrate(H_Away)
            H_Away_m_half = countrateHalf(H_Away_half)
            A_Home_m = countrate(A_Home)
            A_Home_m_half = countrateHalf(A_Home_half)

            H_Away_m_all = nowHomeAll(H_Away_m)
            H_Away_m_allHalf = nowHomeHalf(H_Away_m_half)
            A_Home_m_all = nowAwayAll(A_Home_m)
            A_Home_m_allHalf = nowAwayHalf(A_Home_m_half)

        if len(date_H_Away)>0:
            date_H_Away_m = countrate(date_H_Away)
            date_H_Away_m_half = countrateHalf(date_H_Away_half)
            date_A_Home_m = countrate(date_A_Home)
            date_A_Home_m_half = countrateHalf(date_A_Home_half)

            date_H_Away_m_all = nowHomeAll(date_H_Away_m)
            date_H_Away_m_allHalf = nowHomeHalf(date_H_Away_m_half)
            date_A_Home_m_all = nowAwayAll(date_A_Home_m)
            date_A_Home_m_allHalf = nowAwayHalf(date_A_Home_m_half)

        h6 = (float(len(H_Home)) / (len(H_Home) + len(H_Away))) * 2
        h4 = (float(len(H_Away)) / (len(H_Home) + len(H_Away))) * 2

        # print A_Home_m_all,A_Home_m_allHalf
        if (len(H_Home_m_all)>1) and (len(H_Away_m_all)>1):
            H_All = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(H_Home_m_all,dot6)), map(lambda (a,b):float(a)*float(b)*h4, zip(H_Away_m_all,dot4))))
            H_Half = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(H_Home_m_allHalf,doth6)), map(lambda (a,b):float(a)*float(b)*h4, zip(H_Away_m_allHalf,doth4))))
            # print len(H_All),len(H_Half)
        elif (len(H_Home_m_all) > 1) and (len(H_Away_m_all) == 0):
            H_All = H_Home_m_all
            H_Half = H_Home_m_allHalf
        elif (len(H_Home_m_all) == 0) and (len(H_Away_m_all) >1):
            H_All = H_Away_m_all
            H_Half = H_Away_m_allHalf

        if (len(date_H_Home_m_all)>1) and (len(date_H_Away_m_all)>1):
            date_H_All = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(date_H_Home_m_all,dot6)), map(lambda (a,b):float(a)*float(b)*h4, zip(date_H_Away_m_all,dot4))))
            date_H_Half = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(date_H_Home_m_allHalf,doth6)), map(lambda (a,b):float(a)*float(b)*h4, zip(date_H_Away_m_allHalf,doth4))))
            # print len(H_All),len(H_Half)
        elif (len(date_H_Home_m_all) > 1) and (len(date_H_Away_m_all) == 0):
            date_H_All = date_H_Home_m_all
            date_H_Half = date_H_Home_m_allHalf
        elif (len(date_H_Home_m_all) == 0) and (len(date_H_Away_m_all) >1):
            date_H_All = date_H_Away_m_all
            date_H_Half = date_H_Away_m_allHalf

        if (len(A_Away_m_all) > 1) and (len(A_Home_m_all) > 1):
            A_All = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(A_Away_m_all,dot6)), map(lambda (a,b):float(a)*float(b)*h4, zip(A_Home_m_all,dot4))))
            A_Half = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(A_Away_m_allHalf,doth6)), map(lambda (a,b):float(a)*float(b)*h4, zip(A_Home_m_allHalf,doth4))))
        elif (len(A_Away_m_all) > 1) and (len(A_Home_m_all) == 0):
            A_All = A_Away_m_all
            A_Half = A_Away_m_allHalf
        elif (len(A_Away_m_all) == 0) and (len(A_Home_m_all) > 1):
            A_All = A_Home_m_all
            A_Half = A_Home_m_allHalf

        if (len(date_A_Away_m_all) > 1) and (len(date_A_Home_m_all) > 1):
            date_A_All = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(date_A_Away_m_all,dot6)), map(lambda (a,b):float(a)*float(b)*h4, zip(date_A_Home_m_all,dot4))))
            date_A_Half = map(lambda (a,b):float(a)+float(b), zip(map(lambda (a,b):float(a)*float(b)*h6, zip(date_A_Away_m_allHalf,doth6)), map(lambda (a,b):float(a)*float(b)*h4, zip(date_A_Home_m_allHalf,doth4))))
        elif (len(date_A_Away_m_all) > 1) and (len(date_A_Home_m_all) == 0):
            date_A_All = date_A_Away_m_all
            date_A_Half = date_A_Away_m_allHalf
        elif (len(date_A_Away_m_all) == 0) and (len(date_A_Home_m_all) > 1):
            date_A_All = date_A_Home_m_all
            date_A_Half = date_A_Home_m_allHalf
        # home_count 跟 awaycount 是相反的
        if (len(H_All)>1) and (len(A_All)>1):
            home_count = 0
            for a in range(0,len(H_All)):
                away_count = 0
                al = []
                for b in range(0,len(A_All)):
                    rrate = format((H_All[b]*A_All[a]*100),'.1f')
                    al.append(rrate)
                    if (H_All[b]*A_All[a]*100) > 0 :
                        if home_count == 3 or away_count == 3 :
                            if (H_All[b]*A_All[a]*100) > 3 :
                                sort_3.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(H_All[b]*A_All[a]*100)})
                        else :
                            sort_all.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(H_All[b]*A_All[a]*100)})
                    away_count = away_count + 1
                home_count = home_count + 1
                allRate.append(al)

        if (len(H_Half)>1) and (len(A_Half)>1):
            home_count = 0
            for a in range(0,len(H_Half)):
                away_count = 0
                al = []
                for b in range(0,len(A_Half)):
                    rrate = format((H_Half[b]*A_Half[a]*100),'.1f')
                    al.append(rrate)
                    if (H_Half[b]*A_Half[a]*100) > 0 :
                        if home_count == 3 or away_count == 3 :
                            if (H_Half[b]*A_Half[a]*100) > 3 :  
                                sort_3_h.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(H_Half[b]*A_Half[a]*100)})
                        else :
                            sort_all_h.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(H_Half[b]*A_Half[a]*100)})
                    away_count = away_count + 1
                home_count = home_count + 1    
                allRateHalf.append(al)

        if (len(date_H_All)>1) and (len(date_A_All)>1):
            home_count = 0
            for a in range(0,len(date_H_All)):
                away_count = 0
                al = []
                for b in range(0,len(date_A_All)):
                    rrate = format((date_H_All[b]*date_A_All[a]*100),'.1f')
                    al.append(rrate)
                    if (date_H_All[b]*date_A_All[a]*100) > 0 :
                        if home_count == 3 or away_count == 3 :
                            if (date_H_All[b]*date_A_All[a]*100) > 3 :  
                                sort_3_d.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(date_H_All[b]*date_A_All[a]*100)})
                        else :
                            sort_all_d.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(date_H_All[b]*date_A_All[a]*100)})
                    away_count = away_count + 1
                home_count = home_count + 1    
                date_allRate.append(al)

        if (len(date_H_Half)>1) and (len(date_A_Half)>1):
            home_count = 0
            for a in range(0,len(date_H_Half)):
                away_count = 0
                al = []
                for b in range(0,len(date_A_Half)):
                    rrate = format((date_H_Half[b]*date_A_Half[a]*100),'.1f')
                    al.append(rrate)
                    if (date_H_Half[b]*date_A_Half[a]*100) > 0 :
                        if home_count == 3 or away_count == 3 :
                            if (date_H_Half[b]*date_A_Half[a]*100) > 3 :  
                                sort_3_h_d.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(date_H_Half[b]*date_A_Half[a]*100)})
                        else :
                            sort_all_h_d.append({'比賽日期':ndate,'賽事':GameName,'主隊':home,'客隊':away,'客隊得分':home_count,'主隊得分':away_count,'機率':(date_H_Half[b]*date_A_Half[a]*100)})
                    away_count = away_count + 1
                home_count = home_count + 1    
                date_allRateHalf.append(al)

        # print sort_all
        # print sort_3

        # allRate[0]=[(H_All[0]*A_All[0]),(H_All[1]*A_All[0]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # allRate[1]=[(H_All[0]*A_All[1]),(H_All[1]*A_All[1]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # allRate[2]=[(H_All[0]*A_All[2]),(H_All[1]*A_All[2]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # allRate[3]=[(H_All[0]*A_All[3]),(H_All[1]*A_All[3]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # allRate[4]=[(H_All[0]*A_All[4]),(H_All[1]*A_All[4]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # allRate[5]=[(H_All[0]*A_All[5]),(H_All[1]*A_All[5]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[]),(H_All[]*A_All[])]
        # print H_All

        if len(date_home_data) > 0 and len(home_data) > 0:
            for a in range(0,len(home_data)):
                if a < len (date_home_data):
                    home_data[a] = home_data[a] + [''] + date_home_data[a]
                
        if len(date_away_data) > 0 and len(away_data) > 0:
            for a in range(0,len(away_data)):
                if a < len (date_away_data):
                    away_data[a] = away_data[a] + [''] + date_away_data[a]

        w.writerows(home_data)
        w.writerows(away_data)
        w.writerow(sctitle)
        ii = 0
        it = 0

        for a in allRate:
            ti = []
            ti.append(sctitle1[ii])
            # print a
            if len(date_allRate) > 0 :
                temp = ti + a + [''] + [''] + ti + date_allRate[ii]
                # print date_allRate[ii]
            else :
                temp = ti + a
            # print a,date_allRate[ii]
            w.writerow(temp)
            ii = ii + 1

        w.writerow(sctitle2)
        for a in allRateHalf:
            ti = []
            ti.append(sctitle1[it])
            if len(date_allRateHalf) > 0:
                temp = ti + a + [''] + [''] + [''] + ti + date_allRateHalf[it]
            else :
                temp = ti + a
            # print a,date_allRateHalf[it]
            w.writerow(temp)
            it = it + 1
        funxi.close()


def main():

    # url = 'http://live.leisu.com/wanchang/'
    url = 'http://live.leisu.com/saicheng?date=2017/06/10'
    target = requests.get(url)
    source = BeautifulSoup(target.text.encode("utf-8"), "lxml")

    table = source.find('table', {'class': 'main-content'})

    thead = table.find('thead')
    date = thead.find('span').text

    tbody = table.find('tbody')
    race_list = tbody.findAll('tr')
    print 'game have: ' + str(len(race_list))
    count = 1

    result_dir = 'Result/Analysis/' + date
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)


    alltable = open(result_dir + '/' + date + '總表' + '.csv','w+')
    alltable.write(codecs.BOM_UTF8)
    allw = csv.writer(alltable)
    allw.writerow(['game have:'] + [str(len(race_list))])
    for tr in race_list:
        race_id = tr.get('data-id')
        rtime = tr.find('td', {'class': 'tr-time'}).text
        team_a = tr.find('td', {'class': 'tr-vsl'}).text.replace('\n', '')
        team_b = tr.find('td', {'class': 'tr-vsr'}).text.replace('\n', '')
        GameName = tr.find('td',{'class':'tr-name'}).text
        # score = tr.find('td', {'class': 'tr-score tr-vs-score'}).text.split('-')
        # team_a_score = score[0]
        # team_b_score = score[1]

        # race = {
        #     'Race': race_id,
        #     'Time': time,
        #     'Team_A': {
        #         'Name': team_a,
        #         'Score': team_a_score
        #     },
        #     'Team_B': {
        #         'Name': team_b,
        #         'Score': team_b_score
        #     }
        # }
        # race_file = open(result_dir + '/' + race_id + '.json', 'w+')
        # race_file.write(json.dumps(race, indent=4, sort_keys=True, ensure_ascii=False))
        # race_file.close()
        getRecent(race_id,date,team_a,team_b,GameName)
        allw.writerow([date] + [GameName] + [team_a] + [team_b])
        count = count + 1
        if count % 10 == 0 :
            print count

    alltable.close()

    # 全部全場
    psort_all = sorted(sort_all, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir + '/全部全場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 全部半場
    psort_all = sorted(sort_all_h, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/全部半場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 篩選全場
    psort_all = sorted(sort_all_d, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir + '/篩選全場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 篩選半場
    psort_all = sorted(sort_all_h_d, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/篩選半場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()

    # 全部3分全場
    psort_all = sorted(sort_3, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/全部3分全場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 全部3分半場
    psort_all = sorted(sort_3_h, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/全部3分半場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 篩選3分全場
    psort_all = sorted(sort_3_d, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/篩選3分全場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()
    # 篩選3分半場
    psort_all = sorted(sort_3_h_d, key=lambda x:x['機率'],reverse = True)
    funxi = open(result_dir +'/篩選3分半場-' + date+ '.csv','w+')
    funxi.write(codecs.BOM_UTF8)
    fieldnames = ['比賽日期','賽事','主隊','客隊','主隊得分','客隊得分','機率']
    # w = csv.writer(funxi,fieldnames=fieldnames)
    writer = csv.DictWriter(funxi,fieldnames=fieldnames)
    # print len(psort_all)
    writer.writeheader() 
    for a in psort_all:
        # print a
        writer.writerow(a)
    funxi.close()

    

if __name__ == "__main__":
    main()
    # getRecent('1825575')

