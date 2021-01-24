#!/usr/bin/env python
# coding: utf-8

# In[1]:


#%matplotlib inline
import matplotlib.pyplot as plt
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


# In[2]:




engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')
sql = """
   select * from zt_amount;
"""
df = pd.read_sql(sql, engine)

sql2 = '''
   CREATE TABLE IF NOT EXISTS `unique_table`(
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
       `amount` decimal(12,2) NOT NULL,
       `limit_amount` tinyint unsigned NOT NULL
       )ENGINE=INNODB DEFAULT CHARSET = utf8;
'''
with engine.connect() as con:
   con.execute(sql2)
   
#求得每日最高板
df2 = df.groupby('trade_date',as_index=False).apply(lambda t: t[t.limit_amount==t.limit_amount.max()])
df2.reset_index(drop=True, inplace=True)
#去除有多个并列的，只留唯一标的
df2.drop_duplicates(subset='trade_date',keep=False,inplace=True)
df2.to_sql('unique_table',engine,if_exists='append',index=False)

sql = """
   select * from unique_table;
"""
df = pd.read_sql(sql, engine)

sql3 = '''
   CREATE TABLE IF NOT EXISTS `unique_table_all`(
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
       `amount` decimal(12,2) NOT NULL,
       `limit_amount` tinyint unsigned NOT NULL,
       `open_profit` decimal(5,2),
       `open_price` decimal(6,2),
       `open_rate` decimal(4,2),
       `open_end_profit` decimal(5,2),
       `hit_profit` decimal(5,2),
       `hit_end_profit` decimal(5,2),
       `guxing` decimal(6,2)
       
       )ENGINE=INNODB DEFAULT CHARSET = utf8;
'''
with engine.connect() as con:
   con.execute(sql3)


# In[3]:



def get_hit_profit(series):
    '''打板，-3卖出或尾盘卖出，涨停跌停不卖'''
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    
    #如果是20点封顶
    if pct > 19.9:
        for index, row in df.iterrows():
            
            if buy_price == 0 :  #尚未买入
                if row['open']>=row['pre_close']*1.192:
                    #print("一字开盘，无买点，退出")
                    return np.nan
                if row['high']>=row['pre_close']*1.192:
                    #print("有触及涨停板，打板买入")
                    buy_price = row['high']
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.06  
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['high'] == row['close'] or row['low'] == row['close']:
                    #print("涨停或跌停 继续算下一天")
                    continue
                else:
                    sell_price = row['close'] 
                    #print("尾盘卖出")
                    break
    else:
        #10个点封顶
        for index, row in df.iterrows():
            
            if buy_price == 0 :  #尚未买入
                if row['open']>=row['pre_close']*1.092:
                    #print("一字开盘，无买点，退出")
                    return np.nan
                if row['high']>=row['pre_close']*1.092:
                    #print("有触及涨停板，打板买入")
                    buy_price = row['high']
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.03  
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['high'] == row['close'] or row['low'] == row['close']: 
                    #print("涨停或跌停 继续算下一天")
                    continue
                else:
                    sell_price = row['close'] 
                    #print("尾盘卖出")
                    break
                
    rate = (sell_price-buy_price)/buy_price*100 -2 #摩擦成本2个点
    rate = round(rate,2)
    #print('盈利率：' ,rate)
    return rate

def get_hit_profit_end(series):
    '''打板，尾盘卖出，涨停跌停不卖'''
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    
    #如果是20点封顶
    if pct > 19.9:
        for index, row in df.iterrows():
            
            if buy_price == 0 :  #尚未买入
                if row['open']>=row['pre_close']*1.192:
                    #print("一字开盘，无买点，退出")
                    return np.nan
                if row['high']>=row['pre_close']*1.192:
                    #print("有触及涨停板，打板买入")
                    buy_price = row['high']
                else:
                    return np.nan
            else:#已买入，卖出
                if row['high'] == row['close'] or row['low'] == row['close']:
                    #print("涨停或跌停 继续算下一天")
                    continue
                else:
                    sell_price = row['close'] 
                    #print("尾盘卖出")
                    break
    else:
        #10个点封顶
        for index, row in df.iterrows():
            
            if buy_price == 0 :  #尚未买入
                if row['open']>=row['pre_close']*1.092:
                    #print("一字开盘，无买点，退出")
                    return np.nan
                if row['high']>=row['pre_close']*1.092:
                    #print("有触及涨停板，打板买入")
                    buy_price = row['high']
                else:
                    return np.nan
            else:#已买入，卖出
                
                if row['high'] == row['close'] or row['low'] == row['close']: 
                    #print("涨停或跌停 继续算下一天")
                    continue
                else:
                    sell_price = row['close'] 
                    #print("尾盘卖出")
                    break
                
    rate = (sell_price-buy_price)/buy_price*100 -2 #摩擦成本2个点
    rate = round(rate,2)
    #print('盈利率：' ,rate)
    return rate

def get_hit_price(series):
    '''没用,hit的竞价分析和open的竞价一样'''
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    
    #如果是20点封顶
    if pct > 19.9:
        for index, row in df.iterrows():
            
                if row['open']>=row['pre_close']*1.192:
                    #print("一字开盘，无买点，退出")
                    return np.nan
                if row['high']>=row['pre_close']*1.192:
                    #print("有触及涨停板，打板买入")
                    buy_price = row['high']
                    return buy_price
                else:
                    return np.nan
           
    else:
        #10个点封顶
        for index, row in df.iterrows():
            
            if row['open']>=row['pre_close']*1.092:
                    #print("一字开盘，无买点，退出")
                    return np.nan
            if row['high']>=row['pre_close']*1.092:
                #print("有触及涨停板，打板买入")
                buy_price = row['high']
                return buy_price
            else:
                return np.nan


def get_open_profit2(series):
    '''竞价，尾盘卖出，涨停跌停不卖'''
    #以前用tail(16).head(15)直接去除涨停当天，当不满15条时会失效
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
    #print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    #print(df.iloc[0,:]['trade_date'])
    #涨停的第二天
    if pct < 10.1:#10点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.092:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    else:#20点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.192:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    
    df = df.iloc[1:,:]
    #print('标的涨停后一天已买入，计算再往后的卖点')
    if pct < 10.1:
        #print('已买入')
        for index, row in df.iterrows():#第二天已买入，再后一天及往后推算出卖出
            #print("date: ",row['trade_date'])
            
            
            if row['high'] == row['close'] or row['low'] == row['close']:#涨停或跌停 继续算下一天
                #print('当天涨停或跌停，不卖出，计算下一天')
                continue
            else:
                sell_price = row['close'] #尾盘卖出
                #print('尾盘卖出at ',row['close'])
                break
    else:
        #print('已买入')
        for index, row in df.iterrows():#标的涨停后第二天买入，往后推算出卖出
            #print("date: ",row['trade_date'])
             
            if row['high'] == row['close'] or row['low'] == row['close']:
                #print('当天涨停或跌停，不卖出，计算下一天')
                continue
            else:
                sell_price = row['close'] #尾盘卖出
                #print('尾盘卖出at ',row['close'])
                break
        
      
    rate = (sell_price-buy_price)/buy_price*100 -2 #减去摩擦成本2个点
    rate = round(rate,2)
    #print('盈利率：' ,rate)
    return rate

def get_open_profit(series):
    '''竞价，-3卖出或尾盘卖出，涨停跌停不卖'''
    #以前用tail(16).head(15)直接去除涨停当天，当不满15条时会失效
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
    #print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    #print(df.iloc[0,:]['trade_date'])
    #涨停的第二天
    if pct < 10.1:#10点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.092:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    else:#20点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.192:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    
    df = df.iloc[1:,:]
    #print('标的涨停后一天已买入，计算再往后的卖点')
    if pct < 10.1:
        #print('已买入')
        for index, row in df.iterrows():#第二天已买入，再后一天及往后推算出卖出
            #print("date: ",row['trade_date'])
            loss_stop = row['open'] - row['pre_close']*0.03 #开盘价-3卖出点
            if row['low']< loss_stop:#触发-3卖出
                 
                sell_price = loss_stop
                #print('触发-3卖出at ',sell_price)
                break
            elif row['high'] == row['close'] or row['low'] == row['close']:#涨停或跌停 继续算下一天
                #print('当天涨停或跌停，不卖出，计算下一天')
                continue
            else:
                sell_price = row['close'] #尾盘卖出
                #print('尾盘卖出at ',row['close'])
                break
    else:
        #print('已买入')
        for index, row in df.iterrows():#标的涨停后第二天买入，往后推算出卖出
            #print("date: ",row['trade_date'])
            loss_stop = row['open'] - row['pre_close']*0.06 #开盘价-6卖出止损点
            if row['low']< loss_stop:
                #print('触发-6卖出')
                sell_price = loss_stop
                break
            elif row['high'] == row['close'] or row['low'] == row['close']:
                #print('当天涨停或跌停，不卖出，计算下一天')
                continue
            else:
                sell_price = row['close'] #尾盘卖出
                #print('尾盘卖出at ',row['close'])
                break
        
      
    rate = (sell_price-buy_price)/buy_price*100 -2 #减去摩擦成本2个点
    rate = round(rate,2)
    #print('盈利率：' ,rate)
    return rate

def get_open_price(series):

    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    #print(df.iloc[0,:]['trade_date'])
    #涨停的第二天
    if pct < 10.1:#10点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.092:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    else:#20点封顶的股票
        if df.iloc[0,:]['open']<df.iloc[0,:]['pre_close']*1.192:
            #print("非一字开盘")
            buy_price = df.iloc[0,:]['open']
            #print('买入open价格at ',buy_price)
        else:
            #print('一字开盘，无买点，退出')
            return np.nan
    return buy_price

def get_guxing(series):
    df = pro.daily(ts_code=series['ts_code'], start_date='20090101',end_date=series['trade_date'])
    print(series['trade_date'],end=',')
    if len(df)<30:
        return np.nan
    
    for index, row in df.iterrows():
        if row['pct_chg']<9.92:
            day = row['trade_date']
        else:
            continue
    df = df.loc[df['trade_date']<day,:].head(60)
    zt_amount = len(df.loc[(df['pct_chg']>9.92)&(df['low']!=df['high']),:])
    #guxing = len(df)/zt_amount
    return zt_amount
    
def test():
    pass

 


# In[4]:


#添加open_rate
df['open_rate'] = (df['open_price'] - df['close'])/df['close']*100
df['open_rate'] = df['open_rate'].round(2)
#去除除权造成的错误情况
df = df.loc[(df['open_profit']>-60)|(df['open_rate']>-11),:]


# In[5]:


df["open_end_profit"] = df.apply( get_open_profit2,axis=1)


# In[6]:


df['hit_profit'] = df.apply(get_hit_profit,axis=1)


# In[11]:


df['hit_end_profit'] = df.apply(get_hit_profit_end,axis=1)


# In[12]:


df['guxing'] = df.apply(get_guxing,axis=1)


# In[13]:


df.to_sql('unique_table_all',engine,if_exists='append',index=False)


# In[ ]:




