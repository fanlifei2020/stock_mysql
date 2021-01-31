#!/usr/bin/env python
# coding: utf-8

# In[1]:




import pandas as pd
import numpy as np
import tushare as ts
from time import sleep
from sqlalchemy import create_engine
import sqlalchemy
import pymysql
import os
import datetime

#得到当天日期
today=datetime.date.today()
formatted_today=today.strftime('%y%m%d')
today = '20' + formatted_today

pro = ts.pro_api('7d1f3465439683e262b5b06a8aaefa886ea48aafe2cda73c130beb97')
#df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
#data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
#df = pro.daily(ts_code='000001.SZ,600000.SH', start_date='20180701', end_date='20180718')
#df = pro.daily(trade_date='20180810')



#不显示科学计数法
np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


# In[2]:



engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')

#没有trade_days表就创建
sql = '''
    CREATE TABLE IF NOT EXISTS `trade_days`(
        `day` char(8) NOT NULL
        )ENGINE=INNODB DEFAULT CHARSET = utf8;
'''
with engine.connect() as con:
    con.execute(sql)


# In[3]:


trade_day = pro.trade_cal(exchange='', start_date="20040601",end_date=today)
trade_day = trade_day.loc[trade_day['is_open']==1,:]
trade_day = trade_day[['cal_date']]
trade_day.columns = ['day']
trade_day.to_sql('trade_days',engine,if_exists='replace',index=False)


# In[ ]:




