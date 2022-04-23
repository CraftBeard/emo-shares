import requests as rq
import properties as pr
import logging

class stock:
    def __init__(self, code):
        self.code = code

    def get_post(self):
        url = r'https://xueqiu.com/query/v1/symbol/search/status.json?count={}&comment=0&symbol={}&hl=0&source=all&sort=time&page={}&q=&type=11'.format(20,self.code,1)
        logging.info('crawl data from {}'.format(url))
        content = rq.get(url, headers=pr.HEADERS[0])
        logging.debug('header {}'.format(pr.HEADERS[0]))
        return content

    def get_industry(self):
        url = r'https://xueqiu.com/stock/industry/stockList.json?code={}&type=1&size=100'.format(self.code)
        logging.info('crawl data from {}'.format(url))
        content = rq.get(url, headers=pr.HEADERS[0])
        logging.debug('header {}'.format(pr.HEADERS[0]))
        return content