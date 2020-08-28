"""
doc
"""

import json
import os
import re
import logging
from datetime import datetime, timedelta

import requests

import pandas as pd
from bs4 import BeautifulSoup


def hg_crawler(username, password, result_dir='Results/PD/', to='json'):
    """
    doc
    """

    logging.basicConfig(level=logging.INFO)

    login_link = 'http://www.hg126c.com/app/member/login.php'
    data_link = 'http://www.hg126c.com/app/member/FT_browse/body_var.php'

    if result_dir:
        if not os.path.exists(result_dir):
            logging.info('Create result directory...')
            os.makedirs(result_dir)

    logging.info('Start to crawling...')

    headers = {
        'Host':
        'www.hg126c.com',
        'User-Agent':
        'Mozilla/5.0 \
            (X11; Ubuntu; Linux x86_64; rv:52.0) \
            Gecko/20100101 Firefox/52.0',
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

    logging.info('Login and crawling...')

    response = requests.get('http://www.hg126c.com/', headers=headers)

    session_id = response.cookies.get('PHPSESSID')

    cookies = {
        'username_cookie': username,
        'PHPSESSID': session_id,
    }

    data = [
        ('uid', ''),
        ('langx', 'en-US'),
        ('mac', ''),
        ('ver', ''),
        ('JE', 'false'),
        ('username', username),
        ('password', password),
    ]

    response = requests.post(
        login_link, headers=headers, cookies=cookies, data=data)

    source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")

    params = source.findAll('script')
    params = params[1].text
    params = params.split('\'')[1]
    params = params.split('?')[1]
    params = re.split('[&]*[a-z]+=[\';]*', params)

    uid = params[2]
    langx = params[3]
    mtype = params[1]

    params = (('uid', uid), ('rtype', 'pd'), ('langx', langx), (
        'mtype', mtype), ('delay', ''), ('league_id', ''), ('showtype', ''), )

    response = requests.get(
        data_link, headers=headers, params=params, cookies=cookies)

    source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")

    total_page = re.findall(r'parent.t_page=\d+', source.text)[0]
    total_page = re.findall(r'\d+', total_page)[0]
    total_page = int(total_page)

    logging.info('Total page:' + str(total_page))

    result = []

    for page in range(0, total_page):
        logging.info('Crawling page: ' + str(page + 1))

        if page != 0:

            params = (('uid', uid), ('rtype', 'pd'), ('langx', langx),
                      ('mtype', mtype), ('page_no', page), ('delay', ''), (
                          'league_id', ''), ('showtype', ''), )

            response = requests.get(
                data_link, headers=headers, params=params, cookies=cookies)

            source = BeautifulSoup(
                response.text.encode("utf-8"), "html.parser")

        table = source.findAll('script')[1]
        table = re.split('parent.Game', table.text, re.DOTALL)
        if len(table) < 3:
            return 'No data'
        table = table_parse(table)

        header = table[0]
        header = header.replace('\'', '')
        header = re.findall(r'\(([^"]*)\)', header)
        header = header[0].split(',')

        header = [
            col for index, col in enumerate(header) if index not in [0, 3, 4]
        ]

        for index, value in enumerate(header):
            if value.startswith('ior_'):
                header[index] = value.replace('ior_', '')
                header[index] = header[index].replace('C', 'A')
            else:
                if index == 0:
                    header[index] = 'Date'
                elif index == 1:
                    header[index] = value.title()
                elif index == 2:
                    header[index] = 'Home'
                elif index == 3:
                    header[index] = 'Away'
                elif index == 4:
                    header[index] = value.title()

        for row in table[1:]:
            row = row.replace('\'', '')
            row = re.findall(r'\(([^"]*)\)', row)
            row = row[0].split(',')

            row = [
                value for index, value in enumerate(row)
                if index not in [0, 3, 4]
            ]

            match_pd = {}
            for index, value in enumerate(row):
                match_pd.update({header[index]: value})

            date = match_pd['Date']
            date = re.sub(r'<\w+>', ' ', date)[:-1]
            date = str(datetime.today().year) + '-' + date
            date = datetime.strptime(date, '%Y-%m-%d %H:%M')
            date = date + timedelta(hours=12)

            match_pd['Date'] = int(datetime.timestamp(date))

            if match_pd['Strong'] == 'C':
                match_pd['Strong'] = 'Away'
            else:
                match_pd['Strong'] = 'Home'

            home = {
                'Team': match_pd.pop('Home').strip()
            }

            away = {
                'Team': match_pd.pop('Away').strip()
            }

            for key, value in list(match_pd.items()):
                number_match = re.findall(r'\d', key)

                if len(number_match) < 2:
                    continue

                match_pd.pop(key)
                score_home = number_match[0]
                score_away = number_match[1]

                if score_home >= score_away:
                    home.update({score_home + ':' + score_away: float(value)})
                else:
                    away.update({score_away + ':' + score_home: float(value)})

            home.update({'OT': match_pd.pop('OVH')})
            match_pd.update({'Home': home})
            match_pd.update({'Away': away})
            match_pd.update({'Source': 'hg'})

            result.append(match_pd)

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

        filename = result_dir + str(datetime.now())[:-7] + '_hg'
        json_file = open(filename + '.json', 'w+')
        json_file.write(
            json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
        json_file.close()

    elif to == 'csv':
        logging.info('Output to csv...')

        filename = result_dir + str(datetime.now())[:-7] + '_hg'

        import pandas as pd
        result_table = pd.DataFrame(result)
        result_table = result_table.sort_values(by='Date')
        result_table.to_csv(filename + '.csv', index=False)

    else:
        return False


def table_parse(table):

    last = table[-1]
    table = table[1:-1]

    if last.startswith('FT'):
        table.extend(last.split(';')[0:-13])

    return table


if __name__ == "__main__":
#    hg_crawler(username='calmblue', password='bluesky', result_dir='Results/PD/')
    hg_crawler(username='', password='', to='mongo')
