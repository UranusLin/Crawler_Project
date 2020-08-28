"""
The crawler for williamhill(http://sports.williamhill.com/)
"""

import json
import multiprocessing
import os
import re
import logging
import urllib.parse
from datetime import datetime, timedelta
from functools import partial

import requests
from requests.exceptions import ProxyError, ConnectTimeout

import pandas as pd
from bs4 import BeautifulSoup
from proxies import get_proxy_list


def williamhill_crawler(result_dir=None, to='json', use_proxy=False, proxy=None):
    """
    doc
    """

    logging.basicConfig(level=logging.INFO)

    today_matches_link = 'http://sports.williamhill.com/bet/en-gb/betting/y/5/tm/Football.html'

    if result_dir:
        if not os.path.exists(result_dir):
            logging.info('Create result directory...')
            os.makedirs(result_dir)

    logging.info('Start to crawling...')

    if use_proxy:

        if proxy is not None:

            logging.info('Use provided proxy...')

            response = williamhill_request(today_matches_link, proxy)
            if response is None:
                logging.error('The proxy is not working...')
                return None

        else:
            logging.info('Auto use proxies from spys...')

            #country_list = ['RU', 'ID', 'IN', 'BR', 'TH', 'CN', 'SG', 'TW']
            #country_list = 'CN'
            #proxy_list = get_proxy_list(country_list=country_list)
            proxy_list = get_proxy_list()

            for proxy in proxy_list:
                response = williamhill_request(
                    today_matches_link, proxy, retry=False)

                if response is None:
                    logging.info('Try next proxy...')
                    continue

                else:
                    if not response.ok:
                        logging.error('Connection Error...')
                        logging.info('Try next proxy...')
                        continue

                    else:
                        break

        if response is None:
            logging.error('No proxy available...')
            return None

        if not response.ok:
            logging.error('Connection Error...')
            return None

        williamhill_parser(response, proxy, result_dir, to)

    else:
        response = williamhill_request(today_matches_link)

        williamhill_parser(response, '', result_dir, to)


def williamhill_request(link, proxy='', retry=5):
    """
    doc
    """

    headers = {
        'Host':
        'sports.williamhill.com',
        'User-Agent':
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept':
        'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':
        'zh-TW,en-US;q=0.7,en;q=0.3',
        'DNT':
        '1',
        'Connection':
        'keep-alive',
        'Upgrade-Insecure-Requests':
        '1',
    }

    cookies = {
        'cust_lang':
        'en-gb',
        'cust_prefs':
        'en|ODDS|form|TYPE|PRICE|||0|SB|0|0||0|en|0|TIME|TYPE|0|1|A|0||0|0||TYPE||-|0',
    }

    if proxy:
        logging.info('Use proxy: ' + proxy)

        proxies = {
            'http': 'http://' + proxy,
            'https': 'http://' + proxy,
        }

        for retry_count in range(0, retry):

            try:
                response = requests.get(
                    link,
                    headers=headers,
                    cookies=cookies,
                    proxies=proxies,
                    timeout=10)

            except ProxyError:
                retry_count += 1
                logging.info('Retry...')
                continue

            else:
                return response

    else:

        for retry_count in range(0, retry):

            try:
                response = requests.get(
                    link,
                    headers=headers,
                    cookies=cookies,
                    timeout=10)

            except ConnectTimeout:
                return None

            else:
                return response

def williamhill_parser(response, proxy, result_dir, to):
    """
    doc
    """

    logging.info('Parse match links...')

    source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")

    dm_placeholder = source.find('div', {'id': 'dm_placeholder'})

    if dm_placeholder is None:
        return None

    html_tr = dm_placeholder.findAll('tr')

    match_link_list = []

    for match in html_tr:
        match_link = match.findAll('td', {'class': 'leftPad'},
                                   {'scope': 'col'})

        if not match_link:
            continue

        match_link = match_link[2]
        match_link = match_link.find('a', href=True)
        match_link_list.append(match_link.get('href'))

    logging.info('Total match: ' + str(len(match_link_list)))
    logging.info('Crawling in parallel. Please be patient...')

    pool = multiprocessing.Pool(len(match_link_list))
    func = partial(match_crawler, proxy)
    result = pool.map(func, match_link_list)
    pool.close()

    result = [item for item in result if item is not None]

    if not result:
        logging.error('There is no data...')
        return None

    if to == 'mongo':

        logging.info('Write to MongoDB...')

        from pymongo import MongoClient

        client = MongoClient()
        db = client['crawler']
        coll = db['pd']

        bulkop = coll.initialize_ordered_bulk_op()

        for match in result:

            query = {
                'Date': match['Date'],
                'League': match['League'],
                'Home.Team': match['Home']['Team'],
                'Away.Team': match['Away']['Team'],
                'Source': match['Source']
            }

            retval = bulkop.find(query).upsert().update(
                {
                    '$set': {
                        'Home': match['Home'],
                        'Away': match['Away']
                    }
                }
            )

        retval = bulkop.execute()

    elif to == 'json':
        logging.info('Output to json...')

        filename = result_dir + str(datetime.now())[:-7] + '_williamhill'
        json_file = open(filename + '.json', 'w+')
        json_file.write(
            json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
        json_file.close()

    elif to == 'csv':
        logging.info('Output to csv...')

        filename = result_dir + str(datetime.now())[:-7] + '_williamhill'

        import pandas as pd
        result_table = pd.DataFrame(result)
        result_table = result_table.sort_values(by='Date')
        result_table.to_csv(filename + '.csv', index=False)

    else:
        return False


def match_crawler(proxy, link):
    """
    doc
    """

    if proxy:
        response = williamhill_request(link, proxy)
    else:
        response = williamhill_request(link)

    if response is None:
        return None

    source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")
    cs_items = source.findAll('li', id=re.compile(r'^ip_\d+_cs_container'))

    if not cs_items:
        return None

    date = source.find('span', id='eventDetailsHeader')
    date = date.find('span').text
    date = ' '.join(re.findall(r'\S+', date)[3:-1])

    if not date:
        return None

    date = str(datetime.now().year) + ' ' + date
    date = datetime.strptime(date, '%Y %d %b -%H:%M') + timedelta(hours=7)
    date = datetime.timestamp(date)

    league = source.find('ul', id='breadcrumb')
    league = league.findAll('li')[-1].find('a').text

    match_name = link.split('/')[-1]
    match_name = match_name.split('.')[0]
    match_name = match_name.replace('+', ' ')
    match_name = match_name.split(' v ')

    home = {
        'Team': urllib.parse.unquote_plus(match_name[0].strip())
    }

    away = {
        'Team': urllib.parse.unquote_plus(match_name[1].strip())
    }

    for item in cs_items:
        key = item.find('div', {'class': 'eventselection'})
        key = key.text

        team = re.search(r'^((?!\d-\d).)*', key).group().strip()
        score = re.search(r'\d-\d', key).group()
        score = score.split('-')

        if team == away['Team']:
            score_home = score[1]
            score_away = score[0]
        else:
            score_home = score[0]
            score_away = score[1]

        if int(score_home) > 4 or int(score_away) > 4:
            continue

        ratio = item.find('div', {'class': 'eventprice'})
        ratio = ratio.text
        ratio = re.findall(r'\d+', ratio)

        if ratio:
            ratio = float(ratio[0]) / float(ratio[1])
        else:
            ratio = None

        if score_home >= score_away:
            home.update({score_home + ':' + score_away: ratio})
        else:
            away.update({score_away + ':' + score_home: ratio})

    match_result = {
        'Date': int(date),
        'Home': home,
        'Away': away,
        'League': league,
        'Link': link,
        'Source': 'williamhill'
    }

    return match_result


if __name__ == '__main__':
#    williamhill_crawler(result_dir='Results/PD/')
    williamhill_crawler(to='mongo')
