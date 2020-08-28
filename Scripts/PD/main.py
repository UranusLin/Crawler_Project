"""
doc
"""

import logging
from bingo9 import bingo9_crawler
from hg import hg_crawler
from williamhill import williamhill_crawler


def main():
    """
    doc
    """

    print('Crawling from bingo9...')
#    bingo9_crawler(result_dir='Results/PD/')
    try:
        bingo9_crawler(to='mongo')

    except:
        pass

    print('\nCrawling from hg...')
#    hg_crawler(result_dir='Results/PD/', username='calmblue', password='bluesky')
    try:
        hg_crawler(username='', password='', to='mongo')
    except:
        pass

    print('\nCrawling from williamhill...')
#    williamhill_crawler(result_dir='Results/PD/')
    try:
        williamhill_crawler(to='mongo')
    except:
        pass


if __name__ == '__main__':
    main()
