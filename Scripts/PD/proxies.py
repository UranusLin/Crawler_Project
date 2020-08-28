"""
Crawling the proxies from spys(http://spys.ru/free-proxy-list/)
"""

import requests
from requests.exceptions import ProxyError, ConnectTimeout, ReadTimeout
from bs4 import BeautifulSoup
import logging


def get_proxy_list(country_list='CN', check=False):
    """
    Crawling the proxies from spys(http://spys.ru/free-proxy-list/)
    Input:
        country:
            type: string
            description: which country of proxies. ex: CN, US
        check:
            type: boolean
            description: if ture will check proxy by access youripfast(http://get.youripfast.com/)
    Output:
        type: list
        description: the proxies list.
    """

    logging.info('Crawling proxies...')

    headers = {
        'Host':
        'spys.ru',
        'User-Agent':
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0',
        'Accept':
        'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language':
        'en-US',
        'Content-Type':
        'application/x-www-form-urlencoded',
        'Connection':
        'keep-alive',
        'Upgrade-Insecure-Requests':
        '1',
        'Cache-Control':
        'max-age=0',
    }

    data = [
        ('xpp', '0'),
        ('xf1', '4'),
        ('xf2', '0'),
    ]

    proxy_list = []

    for port in [3128, 8080, 80]:

        if port == 3128:
            data.append(
                ('xf4', '1'), )
        elif port == 8080:
            data.pop()
            data.append(
                ('xf4', '2'), )
        elif port == 80:
            data.pop()
            data.append(
                ('xf4', '3'), )

        if not isinstance(country_list, list):
            country_list = [country_list]

        for country in country_list:

            response = requests.post(
                'http://spys.ru/free-proxy-list/' + country + '/',
                headers=headers,
                data=data)

            source = BeautifulSoup(
                response.text.encode("utf-8"), "html.parser")
            html_td = source.find('td', {'colspan': '10'})
            proxy_table = html_td.findAll('tr')[3:]

            for row in proxy_table:
                proxy_ip = row.find('script')
                if proxy_ip is None:
                    continue

                proxy_ip = proxy_ip.previous_element
                proxy = proxy_ip + ':' + str(port)

                if check:
                    if check_proxy(proxy, headers):
                        proxy_list.append(proxy)

                else:
                    proxy_list.append(proxy)

    return proxy_list


def check_proxy(proxy, headers):
    """
    Check proxy available or not by access youripfast(http://get.youripfast.com/).
    Inputs:
        proxy:
            type: string
            description:
        headers:
            type: dict
            description:
    Output:
        type: string, None
        description: return a proxy if available, None if not.
    """

    logging.info('Check proxy ' + proxy + '...')

    check_proxy_link = 'http://get.youripfast.com/'

    proxies = {
        'http': 'http://' + proxy,
        'https': 'http://' + proxy,
    }

    try:
        response = requests.get(
            check_proxy_link, headers=headers, proxies=proxies, timeout=20)

    except ProxyError:
        logging.error('Proxy Error')
        return False

    except ConnectTimeout:
        logging.error('Connection Timeout')
        return False

    except ReadTimeout:
        logging.error('Read Timeout')
        return False

    if not response.ok or response is None:
        return False

    source = BeautifulSoup(response.text.encode("utf-8"), "html.parser")

    country_flag = source.find('img', {'class': 'country_flag'})

    if country_flag is None:
        logging.error('Proxy is not available')
        return False

    country = country_flag.text

    if country != 'Taiwan':
        return True
