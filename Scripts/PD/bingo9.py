"""
The crawler for Bingo9(https://www.bingo9.net/)
"""

import json
import os
import re
import logging
from datetime import datetime, timedelta
import requests
from requests.exceptions import ProxyError, ConnectTimeout, SSLError, ConnectionError
from proxies import get_proxy_list


def bingo9_crawler(result_dir=None, to='json', use_proxy=False, proxy=None):
    """
    Main function to crawl Bingo9.
    Inputs:
        result_dir:
            type: String
            description: the path to directory used to store the results.
        to_json:
            type: boolean
            description: set to ture will restore the result to json,
                false to csv, the default is true.
        proxy:
            type: string
            description: the proxy used to crawl the Bingo9,
                if False will automatically gather from spys(http://spys.ru/free-proxy-list/).
    Output:
        type: JSON file
        description: contain the pd from Bingo9
    """

    logging.basicConfig(level=logging.INFO)

    if result_dir:
        if not os.path.exists(result_dir):
            logging.info('Create result directory...')
            os.makedirs(result_dir)

    logging.info('Start to crawling...')

    if use_proxy:

        if proxy:

            logging.info('Use provided proxy...')

            response = bingo9_request(proxy)
            if response:
                logging.error('The proxy is not working...')
                return False

        else:
            logging.info('Auto use proxies from spys...')

            proxy_list = get_proxy_list()

            for proxy in proxy_list:
                response = bingo9_request(proxy)

                if not response:
                    logging.info('Try next proxy...')
                    continue

                else:
                    if not response.ok:
                        logging.error('Connection Error...')
                        logging.info('Try next proxy...')
                        continue

                    else:
                        break

    else:
        response = bingo9_request()

    if not response:
        logging.error('Connection Error...')
        return False

    if len(response.text) <= 41:
        logging.error('There is no data...')
        return False

    bingo9_paser(response, result_dir, to)


def bingo9_request(proxy=None):
    """
    A wrapper for send a get request to Bingo9.
    Input:
        type: string
        description: proxy used to send a request
    Output:
        type: requests.response
        description: The response from Bingo9.
    """

    link = 'https://www.bingo9.net/sport/rest/odds/getOddsListBasic.json'

    if proxy:
        logging.info('Use proxy: ' + proxy)
        proxies = {
            'http': 'http://' + proxy,
            'https': 'http://' + proxy,
        }

    cookies = {
        'SESSION_ID': 'guest',
        'LOGINCHK': 'N',
        'vis_first': '1',
        'NP_529678': 'Y',
        'vis_ball': '1',
        'favorite': '{}',
        'langx': 'en-us',
        'GTYPE': 'FT',
        'PTYPE': 'S',
        'L_GameType': 'Intro',
        'page_site': 'Ball',
        'selected_page': 'ball',
        'lang': 'en',
    }

    headers = {
        'Accept-Encoding':
        'gzip, deflate, sdch, br',
        'Accept-Language':
        'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4',
        'User-Agent':
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept':
        'application/json, text/javascript, */*; q=0.01',
        'Referer':
        'https://www.bingo9.net/sport/?game_type=FT',
        'X-Requested-With':
        'XMLHttpRequest',
        'Connection':
        'keep-alive',
    }

    date = datetime.now().timestamp() * 1000
    date = int(date)

    params = (('odds_type', '0'), ('period', 'today'), ('game_type', 'FT'),
              ('play_type', 'PD'), ('cb', 'N'), ('modify_ts', '0'), (
                  'gid_list', '[]'), ('_', date), )

    try:
        if proxy:
            response = requests.get(
                link,
                headers=headers,
                params=params,
                cookies=cookies,
                proxies=proxies,
                timeout=10)

        else:

            response = requests.get(
                link,
                headers=headers,
                params=params,
                cookies=cookies,
                timeout=10)

    except ProxyError:
        logging.error('ProxyError')
        return False

    except ConnectTimeout:
        logging.error('ConnectTimeout')
        return False

    except SSLError:
        logging.error('SSLError')
        return False

    except ConnectionError:
        logging.error('ConnectionError')
        return False

    else:
        return response


def bingo9_paser(response, result_dir, to):
    """
    Parser for extract data from response.
    Inputs:
        response:
            type: requests.response
            description: The response from Bingo9.
        result_dir:
            type: string
            description: the path to directory used to store the results.
        to_json:
            type: boolean
            description: set to ture will restore the result to json,
                false to csv, the default is true.
    """

    logging.info('Parsing data...')

    data = json.loads(response.text)['data']
    insert_list = data['insert']
    game_list = data['game']

    if not isinstance(game_list, dict):
        logging.error('Error: type error')

    game_num = len(game_list.keys())

    logging.info('Total games: ' + str(game_num))
    logging.info('Parsing...')

    result = []

    for game_id, game in game_list.items():
        home = game['tid_h']
        away = game['tid_a']
        league_id = game['lid']
        league = data['league'][league_id]
        strong = 'Away' if game['strong'] == 'C' else 'Home'

        date = datetime.fromtimestamp(game['game_date_timestamp'])
        date = date + timedelta(hours=12)
        date = int(datetime.timestamp(date))

        if league_id not in insert_list:
            continue

        if game_id not in insert_list[league_id]:
            continue

        if 'PD' not in insert_list[league_id][game_id]:
            continue

        home = {
            'Team': home
        }

        away = {
            'Team': away
        }

        bingo9_pd = insert_list[league_id][game_id]['PD']
        for index, key in enumerate(data['odds_map']['PD']):
            key = re.findall(r'\d', key)

            if len(key) < 2:
                home.update({'OT': bingo9_pd[index]})
                continue

            score_home = key[0]
            score_away = key[1]

            if score_home >= score_away:
                home.update({score_home + ':' + score_away: float(bingo9_pd[index])})
            else:
                away.update({score_away + ':' + score_home: float(bingo9_pd[index])})

        match_pd = {
            'Home': home,
            'Away': away,
            'Date': date,
            'League': league,
            'Strong': strong,
            'Source': 'bingo9'
        }

        result.append(match_pd)

    if not result:
        logging.error('There is no data...')
        return False

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

        filename = result_dir + str(datetime.now())[:-7] + '_bingo9'
        json_file = open(filename + '.json', 'w+')
        json_file.write(
            json.dumps(result, indent=4, sort_keys=True, ensure_ascii=False))
        json_file.close()

    elif to == 'csv':
        logging.info('Output to csv...')

        filename = result_dir + str(datetime.now())[:-7] + '_bingo9'

        import pandas as pd
        result_table = pd.DataFrame(result)
        result_table = result_table.sort_values(by='Date')
        result_table.to_csv(filename + '.csv', index=False)

    else:
        return False


if __name__ == '__main__':
#    bingo9_crawler(result_dir='Results/PD/')
    bingo9_crawler(to='mongo')
