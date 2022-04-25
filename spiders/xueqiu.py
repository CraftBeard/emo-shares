from sys import ps1
import requests as rq
import properties as pr
import logging
import mysql.connector as sql
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
import json
from datetime import datetime
import pandas as pd
import time
import random

class stock:
    def __init__(self, code):
        self.code = code
        self.header = pr.HEADERS[0]        
        self.post = ''
        self.industry = ''
        self.dbconfig = pr.DBCONFIG
        self.engine = self.engine = create_engine(r'mysql+pymysql://{}:{}@{}/{}'.format(self.dbconfig['user'],self.dbconfig['pass'],self.dbconfig['host'],self.dbconfig['db']))
        self.max_page = 1
        self.page = 1
        self.next = True
        
    def get_post(self, page=1):
        url = r'https://xueqiu.com/query/v1/symbol/search/status.json?count={}&comment=0&symbol={}&hl=0&source=all&sort=time&page={}&q=&type=11'.format(20,self.code,page)
        logging.info('crawl data from {}'.format(url))
        
        content = rq.get(url, headers=self.header)
        logging.debug('header: {}'.format(self.header))
        logging.debug('post content: {}'.format(content))

        post_list = []
        json_content = json.loads(content.text)

        self.max_page = json_content['maxPage']
        self.page = json_content['page']

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

    def check_exist(self, flag='time'):
        conn = self.engine.connect()
        if flag=='time':
            df_sql = pd.read_sql(sql='select max(create_time) from posts where stock="{}"'.format(self.code), con=conn)
            self.max_time = df_sql
            post = self.post
            post['if_exist'] = post['create_time'].apply(lambda x: True if x<self.max_time.iloc[0,0] else False)
            self.post = post[post['if_exist']==False].drop(columns=['if_exist'])
        elif flag=='url':
            df_sql = pd.read_sql(sql='select url from posts where stock="{}" group by url'.format(self.code), con=conn)
            df_sql['if_exist'] = True
            post = self.post
            post = pd.merge(post, df_sql, how='left', on='url')
            self.post = post[post['if_exist']!=True].drop(columns=['if_exist'])
        conn.close()

    def check_next(self, flag='cnt'):
        if flag=='cnt':
            if len(self.post)==0:
                self.next=False
        elif flag=='page':
            if self.page>=2 and self.page>=self.max_page:
                self.next=False

    def insert_db(self):                     
        conn = self.engine.connect()
        logging.info('insert count {}'.format(len(self.post)))
        if len(self.post)>0:
            self.post.to_sql(name='posts', con=conn, if_exists='append', index=False)
        conn.close()

    def crawl_data(self, flag):
        
        while self.page<=self.max_page and self.next==True:
            logging.info('page {}'.format(self.page))
            logging.info('max_page {}'.format(self.max_page))
            logging.info('next {}'.format(self.next))
            self.get_post(self.page)
            if flag=='init':
                self.check_exist('url')
                self.insert_db()
                self.check_next('page')
            else:
                self.check_exist('time')
                self.insert_db()
                self.check_next('cnt')
            self.page += 1

            sleep_time = random.uniform(1, 15)
            logging.info('wait {}s'.format(sleep_time))
            time.sleep(sleep_time)
