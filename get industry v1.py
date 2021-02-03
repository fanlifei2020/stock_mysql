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


# In[19]:



engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')

 


# In[20]:



# In[21]:

"""
数据库导入
sql = "select * from trade_days;"
trade_day = pd.read_sql(sql,engine)
trade_day = trade_day['day'].to_list()
"""
#tushare导入
trade_day = pro.trade_cal(exchange='', start_date='20040601',end_date=today)
trade_day = trade_day.loc[trade_day['is_open']==1,:]
trade_day = trade_day[['cal_date']]
trade_day = trade_day['cal_date'].tolist()


# In[50]:


def get_zf(series):
    #不放入内存计算的话，太慢了
    #df = pro.daily(ts_code=series['ts_code'], end_date=series['trade_date']) 
    #print(series['ts_code'],end=",")
    #df = work_df.loc[(work_df['ts_code']=series['ts_code'])&(work_df['trade_date']<=series['trade_date']),:]
    df = work_df.loc[(work_df['ts_code']==series['ts_code'])&(work_df['trade_date']<=series['trade_date']),:]
    #print("*",end="")
    if len(df)<30:
        return -100
    return df.head(20).pct_chg.sum()


# In[55]:


data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')


# In[56]:


date = ""
date_list = []
popular_industry_list = []
while date<"20210101":

    if date=="":
        print("date is null")
        work_sql = "select * from daily where trade_date>" + "20090901" + " and trade_date<" + "20101201;"
        work_df = pd.read_sql(work_sql,engine)
        last_index = trade_day.index('20091231')
        
    else:
        
        
        #构造日线工作df至内存，加快算速。取目标交易日（前30天——后260天）所有a股日线为工作df
        last_index = trade_day.index(date) #不能省，设置last_index，后面循环会用到
        start_index = last_index - 30          
        if date>"20200101":
            work_sql = "select * from daily where trade_date>" + trade_day[start_index] +";"
        else:
            work_sql = "select * from daily where trade_date>" + trade_day[start_index] + " and trade_date<" + trade_day[last_index+260] + ";"     
        work_df = pd.read_sql(work_sql,engine)
        
        
    
    #空表从20050104开始算，
    print("start: ",trade_day[last_index+1])   
    stop = 0
    for day in trade_day[last_index+1:]:
        if stop>250:
            print("250 arrived")
            break
        #data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
        daily_df = work_df.loc[work_df['trade_date']==day,:]
        result = pd.merge(daily_df,data)
        result['zf'] = result.apply(get_zf,axis=1)

        cut = len(result)//5
        popular_industry = result.sort_values('zf',ascending=False).head(cut)['industry'].value_counts().index[0]
        date_list.append(day)
        popular_industry_list.append(popular_industry)
        print(day,end=",")
        date = day
        
    del work_df


# In[ ]:


for day in trade_day[start_index:]:
    print(day)
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
    daily_df = work_df.loc[work_df['trade_date']=='20180810',:]
    result = pd.merge(daily_df,data)
    result['zf'] = result.apply(get_zf,axis=1)
    
    cut = len(result)//5
    result.sort_values('zf',ascending=False).head(cut)['industry'].value_counts().index[0]


# In[17]:


data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,industry')
#df = pro.daily(ts_code='000001.SZ,600000.SH', start_date='20180701', end_date='20180718')
#df = pro.daily(trade_date='20180810')


# In[2]:


#df = pro.daily(trade_date='20180810')


# In[18]:



daily_df = work_df.loc[work_df['trade_date']=='20180810',:]


# In[19]:


daily_df


# In[20]:


result = pd.merge(daily_df,data)


# In[21]:


result


# In[24]:


result['zf'] = result.apply(get_zf,axis=1)


# In[29]:


cut = len(result)//5


# In[ ]:





# In[ ]:





# In[38]:


result.sort_values('zf',ascending=False).head(cut)['industry'].value_counts().index[0]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




