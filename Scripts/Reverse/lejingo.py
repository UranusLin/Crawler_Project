import requests
import json
import logging
import sys
import os
import pandas as pd
#import multiprocessing
import time
from datetime import datetime
from bs4 import BeautifulSoup
from pymongo import MongoClient


class LejingoCrawler:

    def __init__(self, username, password, to='json', result_dir=None, log_level='INFO'):

        self.logger = self.lejingo_logger(log_level=log_level)
        start_time = time.time()
        self.login_session = requests.Session()

        self.headers = {
#            'Host': 'otto.lejingo.com',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }

        self.login_session.headers.update(self.headers)

        self.response = self.lejingo_login(username=username, password=password)
        self.response = self.get_reverse()
        self.result = self.reverse_parser()

        if to == 'mongo':
            self.write_to_mongo()
        elif to in ['json', 'csv']:
            if not result_dir:
                ValueError('Please input your result_dir')

            os.makedirs(result_dir, exist_ok=True)

            if to == 'json':
                self.write_to_json(result_dir)
            elif to == 'csv':
                self.write_to_csv(result_dir)

        else:
            Exception('Output option error')

        elapsed_time = time.time() - start_time
        self.logger.info('Crawler job complete, elapsed time: ' + str(elapsed_time))

    def lejingo_logger(self, log_level='INFO'):
        logger = logging.getLogger(__name__)
        logger.handlers = []
        if log_level == 'INFO':
            logger.setLevel(logging.INFO)
        elif log_level == 'DEBUG':
            logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.propagate = False

        return logger

    def lejingo_login(self, username, password):

        self.login_session.headers.update({'Content-Type': 'application/x-www-form-urlencoded'})

        while True:
            data = [
                ('user', username),
                ('pwd', password),
                ('verify_code', self.ocr_verify_code()),
                ('enter', 'enter'),
            ]

            self.logger.debug('Login data: ' + str(data))
            response = self.login_session.post('http://otto.lejingo.com/index.php', data=data)

            if response.ok:
                source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")
                login_frm = source.find('div', {'class': 'login_frm'})
                topM = login_frm.find('ul', {'class': 'topM'})

                if topM:
                    break

        return response

    def ocr_verify_code(self):
        pic_url = 'http://otto.lejingo.com/showpic.php'
        image = self.login_session.get(pic_url)
        verify_code = requests.post('http://localhost:8088/image_to_number', files={'image': image.content})
        verify_code = json.loads(verify_code.text)
        self.logger.debug('Verify code: ' + verify_code['Result'])

        if len(verify_code['Result']) == 4:
            return verify_code['Result']

        return self.ocr_verify_code()

    def get_reverse(self):

        self.logger.info('Crawling revers data')
        response = self.login_session.get('http://otto.lejingo.com/goKS.php', allow_redirects=False)

        self.logger.info('Create reverse session')
        self.reverse_session = requests.Session()
        self.reverse_session.headers.update(self.headers)

        response = self.reverse_session.get(response.headers['Location'],cookies=response.cookies)
        self.logger.info('Reverse data crawled')

        return response

    def reverse_parser(self):

        self.logger.info('Parsing reverse data')
        source = BeautifulSoup(
                self.response.text.encode("utf-8"), "html.parser")

        daily_bet_list = source.find('ul', {'class': 'bet_list'}).find('li').find('ul')

        match_list = daily_bet_list.findAll('li')
        self.logger.info('Total match: ' + str(len(match_list)))

        result = [self.match_parser(match) for match in match_list[0:1]]

        self.logger.info('Data parsed')

#        self.logger.info('Parse data in multiprocess')
#        pool = multiprocessing.Pool(len(match_list))
#        result = pool.map(self.match_parser, match_list)
#        pool.close()
#        pool.join()

        return result

    def match_parser(self, match):

        reverse_root = 'http://' + self.reverse_session.cookies.list_domains()[0] + '/'

        date = match.find('div', {'class': 'date'}).text
        date = date.replace('&nbsp', ' ').replace(';', '')
        year = str(datetime.now().year)
        date = datetime.strptime(year + '/' + date, '%Y/%m/%d %H:%M')
        date = int(date.timestamp())

        match_data ={
            'League': match.find('div', {'class': 'name'}).text,
            'Date': date,
            'Home': {'Team': match.find('div', {'class': 'vs'}).text.split('vs')[0].strip()},
            'Away': {'Team': match.find('div', {'class': 'vs'}).text.split('vs')[1].strip()},
            'Link': reverse_root + match.find('a', href=True)['href'],
            'Source': 'lejingo'
        }

        self.logger.debug(match_data)

        response = self.reverse_session.get(match_data['Link'])

        source = BeautifulSoup(
            response.text.encode("utf-8"), "html.parser")

        reverse = source.find('div', {'class': 'col-lg-9 zero'})

        full = reverse.findAll('table', {'class': 'table bet_color'})[0]
        full = self.table_parser(full)
        self.logger.debug('Full: ' + str(full))

        half = reverse.findAll('table', {'class': 'table bet_color'})[1]
        half = self.table_parser(half)
        self.logger.debug('Half: ' + str(half))

        match_data.update({'Full': full, 'Half': half})

        return match_data

    def table_parser(self, table):

        result = {}
        for row in table.findAll('tr')[1:]:
            key = row.findAll('td')[0].text

            if not key[0].isdigit():
                key = 'OT'

            else:
                key = key.replace(' ', '')
                key = key.replace('-', ":")

            value = row.findAll('td')[1].text
            value = round(float(value.strip('%'))/100, 4)

            result.update({key: value})

        return result

    def write_to_mongo(self):
        self.logger.info('Write to MongoDB...')

        client = MongoClient()
        db = client['crawler']
        coll = db['reverse']

        bulkop = coll.initialize_ordered_bulk_op()

        for match in self.result:

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
                        'Full': match['Full'],
                        'Half': match['Half']
                    }
                }
            )

        retval = bulkop.execute()

    def write_to_json(self, result_dir):
        self.logger.info('Output to json...')

        filename = result_dir + '/' + str(datetime.now())[:-7] + '_lejingo'
        with open(filename + '.json', 'w+') as json_file:
            json_file.write(
                json.dumps(self.result, indent=4, sort_keys=True, ensure_ascii=False))

    def write_to_csv(self, result_dir):
        self.logger.info('Output to csv...')

        filename = result_dir + '/' + str(datetime.now())[:-7] + '_lejingo'

        result_table = pd.DataFrame(result)
        result_table = result_table.sort_values(by='Date')
        result_table.to_csv(filename + '.csv', index=False)


if __name__ == '__main__':
#    crawler = LejingoCrawler(username='otto72', password='aa1234', result_dir='Results/Reverse', log_level='DEBUG')
    crawler = LejingoCrawler(username='', password='', to='mongo', log_level='DEBUG')
