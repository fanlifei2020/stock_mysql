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
trade_day = pro.trade_cal(exchange='', start_date='20100101',end_date=today)
trade_day = trade_day.loc[trade_day['is_open']==1,:]
trade_day = trade_day[['cal_date']]
trade_day = trade_day['cal_date'].tolist()

#不显示科学计数法
np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.2f' % x)


# In[2]:


def get_amount(series):
#取得15天日线,从涨停当天开始往前推算
    df = work_df.loc[(work_df['ts_code']==series['ts_code'])&(work_df['trade_date']<=series['trade_date']),:]
    sorted_df = df.sort_values(by="trade_date",ascending=False).head(15)
    #df = pro.daily(ts_code=series['ts_code'], end_date=series['trade_date']).head(15)
    #print("15日df created")
    
    #df = pd.read_sql(sql,engine)
    #print(df)
    #print("\n")
    count = 0
    flag = 0
    #print('ts_code:',row['ts_code'])
 
    for index, row in sorted_df.iterrows():
        #print(row['trade_date']+"----"+row['ts_code'])
        if row['pct_chg']<9.9 or (row['pct_chg']>10.1 and row['pct_chg']<19.9) or row['pct_chg']>20.1:
            #print("涨幅判断未涨停，退出",end=',')
            return count
        if row['high']>row['close']*1.005:
            #print("最高价大于收盘价判断未涨停，退出",end=',')
            return count
        
        #以下都是涨停板
        if row['low'] == row['high']:
            #print("一字板,flag+")
            if row['pct_chg']<10.1:
                flag = flag + 1
                #print("flag=",flag)
            else:
                flag = flag + 2
                #print("flag=",flag)
        else:
            #非一字板，包括自然板和T字板
            #不用有量无量判断，因为有接近无量的自然板
            
            if row['open']==row['high']:
                if row['amount']>30000:
                #金额大于3千万，有量T字板,当自然板算
                    #print('有量T字板',end=',')
                    if row['pct_chg']<10.1:
                        count = count + 1 + flag
                        flag=0
                        #print("count=",count)
                    else:
                        count = count + 2 + flag
                        flag=0
                        #print("count=",count)
                else:
                #无量T字板
                    #print('无量T字板')
                    if row['pct_chg']<10.1:
                        flag = flag + 1
                        #print("flag=",flag)
                    else:
                        flag = flag + 2
                        #print("flag=",flag)
            else:
                #非T字板
                #print('自然板，结算',end=',')
                if row['pct_chg']<10.1:
                    count = count + 1 + flag
                    flag=0
                    #print("count=",count)
                else:
                    count = count + 2 + flag
                    flag=0
                    #print("count=",count)
            
    #print("count=",count)
    return count


# In[3]:


engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')

#没有表就创建
sql = '''
    CREATE TABLE IF NOT EXISTS `zt_amount`(
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
    con.execute(sql)


# In[4]:


date = ""
while date<"20210101":
    print("date: ",date)
    temp_sql =  """select * from zt_amount order by trade_date desc limit 1;"""
    amount_df = pd.read_sql(temp_sql, engine)

    last_day = ""
    last_index = 0
    if len(amount_df)>0:
        print("amount>0")
        last_day = amount_df['trade_date'].tolist()[-1]
        last_index = trade_day.index(last_day)
        print("last_day:",last_day)
    
    if last_index < 30:
        start = '20100101'
    else:
        start = trade_day[last_index-30]
        
    if date<"20200401":
        work_sql = "select * from daily where trade_date>" + start + " and trade_date<" + trade_day[last_index+260] + ";"
        work_df = pd.read_sql(work_sql,engine)
    else:
        work_sql = "select * from daily where trade_date>" + start +";"
        work_df = pd.read_sql(work_sql,engine)
    #work_sql = "select * from daily where trade_date>" + start +";"
    #work_df = pd.read_sql(work_sql,engine)

    if last_day < today and last_day != "" :
        start_index = last_index +1
    else: 
        start_index = 0
    print('start_index: ',start_index)
    print('start_day:',trade_day[start_index])

    stop = 0 
    for day in trade_day[start_index:]:
        #print("trade_day:",day)
        if stop>250:
            print("250 arrived")
            break
        day_sql = "select * from zt where trade_date = " + day + ";"
        day_df = pd.read_sql(day_sql,engine)
        #print(day_df)
        if len(day_df)==0:
            continue
        day_df['limit_amount'] = day_df.apply(get_amount,axis=1)
        day_df = day_df.loc[day_df['limit_amount']>1,:]
        day_df.to_sql('zt_amount',engine,if_exists='append',index=False)
        print(day,end=",")
        stop += 1
        date = day
    del work_df


# In[ ]:




