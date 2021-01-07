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
"""
2020年8月后创业板开通20%涨幅，算法要改，目前是20%算作2个板来计算最高板，卖法未完善，待改进
"""



#不显示科学计数法
np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


def get_limit(day):
    df = pro.daily(trade_date=day)
    df = df.loc[df['pct_chg']>9.9,:]
    return df


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
     
#如果有表，从上次位置添加新的记录
date_df = pd.read_sql('trade_days', engine)
if len(date_df)>0:
    last_day = date_df.tail(1)['day'].tolist()[0]
    if last_day != today:
        trade_day = pro.trade_cal(exchange='', start_date=last_day,end_date=today)
        trade_day = trade_day.loc[trade_day['is_open']==1,:]
        trade_day = trade_day[['cal_date']]
        trade_day.columns = ['day']
        trade_day.to_sql('trade_days',engine,if_exists='append',index=False)
else:
    trade_day = pro.trade_cal(exchange='', start_date='20100101',end_date=today)
    trade_day = trade_day.loc[trade_day['is_open']==1,:]
    trade_day = trade_day[['cal_date']]
    trade_day.columns = ['day']
    trade_day.to_sql('trade_days',engine,if_exists='append',index=False)


# In[3]:


#没有trade_days表就创建
sql = '''
    CREATE TABLE IF NOT EXISTS `daily`(
        `ts_code` varchar(20) NOT NULL,
        `trade_date` varchar(8) NOT NULL,
        `open` decimal(6,2) NOT NULL,
        `high` decimal(6,2) NOT NULL,
        `low` decimal(6,2) NOT NULL,
        `close` decimal(6,2) NOT NULL,
        `pre_close` decimal(6,2) NOT NULL,
        `change` decimal(6,2) NOT NULL,
        `pct_chg` decimal(6,2) NOT NULL,
        `vol` decimal(12,2) NOT NULL,
        `amount` decimal(12,2) NOT NULL        
        )ENGINE=INNODB DEFAULT CHARSET = utf8;
'''
with engine.connect() as con:
    con.execute(sql)


# In[5]:


print('444')
sql = """
    select * from daily order by trade_date desc limit 10;
"""
daily_df = pd.read_sql(sql, engine)
if len(daily_df)>0:
    last_day = daily_df['trade_date'].tolist()[-1]
    if last_day != today:
        print('123')
        trade_day = pro.trade_cal(exchange='', start_date=last_day,end_date=today)
        trade_day = trade_day.loc[trade_day['is_open']==1,:]
        trade_day = trade_day[['cal_date']]
        trade_day = trade_day['cal_date'].tolist()
        #trade_day.index(last_day)
        for day in trade_day[1:]:
            df = pro.daily(trade_date=day)
            print(day,end=',')
            sleep(5)
            df.to_sql('daily',engine,if_exists='append',index=False)
else:
    trade_day = pro.trade_cal(exchange='', start_date='20100101',end_date=today)
    trade_day = trade_day.loc[trade_day['is_open']==1,:]
    trade_day = trade_day[['cal_date']]
    trade_day = trade_day['cal_date'].tolist()
    for day in trade_day:
        df = pro.daily(trade_date=day)
        print(day,end=',')
        sleep(5)
        df.to_sql('daily',engine,if_exists='append',index=False)


# In[21]:



if last_day is not None:
    trade_day = pro.trade_cal(exchange='', start_date=last_day,end_date=today)
    trade_day = trade_day.loc[trade_day['is_open']==1,:]
    trade_day = trade_day[['cal_date']]
    trade_day.columns = [['day']]
    trade_day.to_sql('trade_days',engine,if_exists='append',index=False)
else:
    trade_day = pro.trade_cal(exchange='', start_date='20100101',end_date=today)
    trade_day = trade_day.loc[trade_day['is_open']==1,:]
    trade_day = trade_day[['cal_date']]
    trade_day.columns = [['day']]
    trade_day.to_sql('trade_days',engine,if_exists='append',index=False)
    
    


# In[19]:


trade_day


# In[20]:


date_df.head()


# In[15]:


trade_day.columns = [['day']]


# In[16]:


trade_day


# In[ ]:





# In[52]:


trade_days[2914]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[9]:


def calculate_profit(series):
    
    """问题：停牌的股票也会推后算，这怎么算"""
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']).tail(16).head(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
   # print('股票：',series['ts_code'])
    pct = series['pct_chg']
    buy_flag = 0
    buy_price = 0
    sell_price = 0
    for index, row in df.iterrows():
      #  print('日期：', row['trade_date'])
       # print("**************")   
        if pct<11:
            if buy_flag == 0 :  #没买则买入
               # print('尚未买入')
                if row['open']>row['pre_close']*1.093:#一字开盘没买点，不买
                  #  print('一字板开盘，无买点，退出')
                    return np.nan
                else:
                    buy_price = row['open']#非一字开盘竞价入
                    buy_price_rate = (buy_price-row['pre_close'])/row['pre_close']
                  #  print('非一字开盘，有买点，买入价格： ',buy_price)
                    buy_flag = 1   #已买
            else:  #如果已买
                #几种情况要分别计算1.跌停不卖 2.跌停卖
              #  print('已经买入')
                #-3卖出
                if row['low']<row['open']*(1-0.03):

                    sell_price = row['open']*(1-0.03)
                  #  print('-3卖出,价格：',sell_price)
                    break
                #没有-3
                else:
                    if (row["pct_chg"]>9.3) |(row["pct_chg"]<-9.3 ):
                        #print('当天涨停或跌停，继续算下一天的卖点')
                        continue
                    else:
                        sell_price = row['close']
                       # print('无-3，无涨停，尾盘卖出,价格：',sell_price)
                        break
                        
        if pct>19:
            if buy_flag == 0 :  #没买则买入
               # print('尚未买入')
                if row['open']>row['pre_close']*1.193:#一字开盘没买点，不买
                  #  print('一字板开盘，无买点，退出')
                    return np.nan
                else:
                    buy_price = row['open']#非一字开盘竞价入
                    buy_price_rate = (buy_price-row['pre_close'])/row['pre_close']
                  #  print('非一字开盘，有买点，买入价格： ',buy_price)
                    buy_flag = 1   #已买
            else:  #如果已买
                #几种情况要分别计算1.跌停不卖 2.跌停卖
              #  print('已经买入')
                #-3卖出
                if row['low']<row['open']*(1-0.06):

                    sell_price = row['open']*(1-0.06)
                  #  print('-6卖出,价格：',sell_price)
                    break
                #没有-3
                else:
                    if (row["pct_chg"]>19.3) |(row["pct_chg"]<-19.3 ):
                        #print('当天涨停或跌停，继续算下一天的卖点')
                        continue
                    else:
                        sell_price = row['close']
                       # print('无-3，无涨停，尾盘卖出,价格：',sell_price)
                        break

    rate = (sell_price-buy_price)/buy_price*100 -2
    rate = round(rate,2)
    #print('盈利率：' ,rate)
    return rate

def get_buy_price(series):
    
    """问题：停牌的股票也会推后算，这怎么算"""
    pct = series['pct_chg']
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']).tail(16).head(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
   # print('股票：',series['ts_code'])
   
    buy_flag = 0
    buy_price = 0
    sell_price = 0
    for index, row in df.iterrows():
      #  print('日期：', row['trade_date'])
       # print("**************")   
       
        if buy_flag == 0 :  #没买则买入
           # print('尚未买入')
            if pct>19:
                if row['open']>row['pre_close']*1.193:
                    return np.nan
                else:
                    buy_price = row['open']#非一字开盘竞价入
                    return buy_price
            else:
                if row['open']>row['pre_close']*1.093:#一字开盘没买点，不买
              #  print('一字板开盘，无买点，退出')
                    return np.nan
                else:
                    buy_price = row['open']#非一字开盘竞价入
                    return buy_price
                
        
        
        
        
        
           


        


# In[10]:


day_df = pd.DataFrame({"day":trade_days})


# In[11]:


day_df.head()


# In[23]:


len(day_df)


# In[ ]:





# In[13]:


engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')
#df = pd.read_sql('emp_master', engine)


day_df.to_sql('trade_days', engine)


# In[19]:


day_df[:5]


# In[21]:


df = pro.daily(trade_date='20100104')


# In[22]:


df


# In[ ]:





# In[27]:


for day in trade_days[4:5]:
    df = pro.daily(trade_date=day)
    print(df.head(1))
    df.to_sql('daily',engine,if_exists='append',index=False)


# In[28]:


record = pd.read_sql('daily', engine)


# In[38]:


yesterday = record.tail(1)['trade_date'].tolist()[0]


# In[39]:


new_start = trade_days.index(yesterday)


# In[40]:


#add trade_date
engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')
#df = pd.read_sql('emp_master', engine)
last_day_df = pd.read_sql('trade_days', engine)
last_day = last_day_df.tail(1)['day'].tolist()[0]


trade_day = pro.trade_cal(exchange='', start_date=last_day)
trade_day = trade_day.loc[trade_day['is_open']==1,:]
trade_days = list(trade_day['cal_date'])
"""


# In[45]:


last_day.tail(1)['day'].tolist()[0]


# In[ ]:





# In[ ]:





# In[ ]:





# In[53]:


import datetime


# In[54]:


import datetime
today=datetime.date.today()
formatted_today=today.strftime('%y%m%d')
today = '20' + formatted_today


# In[56]:


today = '20' + formatted_today


# In[57]:


today


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


#add daily


# In[ ]:





# In[ ]:





# In[ ]:


#add jisuan 


# In[ ]:





# In[ ]:





# In[ ]:




