#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pymongo
from bs4 import BeautifulSoup
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# In[9]:


base = declarative_base()
class NaverKeywords(base):
    __tablename__ = "naver"

    id = Column(Integer, primary_key=True)
    rank = Column(Integer, nullable=False)
    keyword = Column(String(50), nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)

    def __init__(self, rank, keyword):
        self.rank = rank
        self.keyword = keyword

    def __repr__(self):
        return "<NaverKeywords {}, {}>".format(self.rank, self.keyword)  


# In[3]:


# 클래스 작성
# 생성자, 크롤링함수, mysql 저장, mongodb 저장, 슬랙전송, 실행함수 작성
# __init__, crawling, mysql_save, mongo_save, send_slack, run


# In[18]:


class NaverKeywordsCrawling:

    def __init__(self, base, ip="52.78.189.210", pw="dssf", database="terraform"):
        self.mysql_client = create_engine("mysql://root:{}@{}/{}?charset=utf8".format(pw, ip, database))
        self.mongo_client = pymongo.MongoClient('mongodb://{}:27017'.format(ip))
        self.datas = None
        self.base = base
        
    def crawling(self):
        response = requests.get("https://www.naver.com/")
        dom = BeautifulSoup(response.content, "html.parser")
        keywords = dom.select(".ah_roll_area > .ah_l > .ah_item")
        datas = []
        for keyword in keywords:
            rank = keyword.select_one(".ah_r").text
            keyword = keyword.select_one(".ah_k").text
            datas.append((rank, keyword))
        self.datas = datas
        
    def mysql_save(self):
        
        # make table
        self.base.metadata.create_all(self.mysql_client)
        
        # parsing keywords
        keywords = [NaverKeywords(rank, keyword) for rank, keyword in self.datas]

        # make session
        maker = sessionmaker(bind=self.mysql_client)
        session = maker()

        # save datas
        session.add_all(keywords)
        session.commit()

        # close session
        session.close()
        
    def mongo_save(self):
        
        # parsing querys
        keyowrds = [{"rank":rank, "keyword":keyword} for rank, keyword in self.datas]
        
        # insert keyowrds
        self.mongo_client.terraform.naver_keywords.insert(keyowrds)
        
    def send_slack(self, msg, channel="#dssf", username="provision_bot" ):
        webhook_URL = "https://hooks.slack.com/services/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"
        payload = {
            "channel": channel,
            "username": username,
            "icon_emoji": ":provision:",
            "text": msg,
        }
        response = requests.post(
            webhook_URL,
            data = json.dumps(payload),
        )
        return response
    
    def run(self):
    
        # crawling
        self.crawling()

        # save datas to db
        self.mysql_save()
        self.mongo_save()

        # send msg
        self.send_slack("naver crawling done!")


# In[19]:


nk = NaverKeywordsCrawling(base)


# In[20]:


nk.run()

