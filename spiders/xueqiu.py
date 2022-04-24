from sys import ps1
import requests as rq
import properties as pr
import logging
import mysql.connector as sql
import pymysql
import json
from datetime import datetime
import pandas as pd

class stock:
    def __init__(self, code):
        self.code = code
        self.header = pr.HEADERS[0]        
        self.post = ''
        self.industry = ''
        self.dbconfig = pr.DBCONFIG
        
    def get_post(self):
        url = r'https://xueqiu.com/query/v1/symbol/search/status.json?count={}&comment=0&symbol={}&hl=0&source=all&sort=time&page={}&q=&type=11'.format(20,self.code,1)
        logging.info('crawl data from {}'.format(url))
        
        content = rq.get(url, headers=self.header)
        logging.debug('header: {}'.format(self.header))
        logging.debug('post content: {}'.format(content))

        post_list = []
        json_content = json.loads(content.text)
        for post in json_content['list']:
            post_list.append({
                'stock':json_content.get('q', 'not exists'),
                'user_id':post.get('user', 'not exists').get('id', 'not exists'),
                'user_name':post.get('user', 'not exists').get('screen_name', 'not exists'),
                'following':post.get('user', 'not exists').get('friends_count', 'not exists'),
                'follower':post.get('user', 'not exists').get('followers_count', 'not exists'),
                'create_time':datetime.fromtimestamp(int(post.get('created_at', 'not exists')/1000)),
                'url':'https://xueqiu.com'+post.get('target', 'not exists'),
                'content':post.get('text', 'not exists'),
            })

        self.post = pd.DataFrame(post_list)

    def get_industry(self):
        url = r'https://xueqiu.com/stock/industry/stockList.json?code={}&type=1&size=100'.format(self.code)
        logging.info('crawl data from {}'.format(url))
        content = rq.get(url, headers=self.header)
        logging.debug('header: {}'.format(self.header))
        logging.debug('industry content: {}'.format(content))
        self.industry = content

    def insert_db(self):
        conn = pymysql.connect(host=self.dbconfig['host'],
                       port=self.dbconfig['port'],
                       user=self.dbconfig['user'], 
                       passwd=self.dbconfig['pass'],  
                       db=self.dbconfig['db'])
        self.post.to_sql(name='posts', con=conn, if_exists='append', index=False)