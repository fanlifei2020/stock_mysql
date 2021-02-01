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


# In[55]:


def first_yin(series):
    '''当天收绿买入'''
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
                if (row['open'] - row['close'])/row['pre_close']>0.20:
                    buy_price = row['close']
                elif row['pct_chg']>19.9:
                    continue
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.06  
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['pct_chg']>19.9 or row['pct_chg']<-19.9:
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
                if (row['open'] - row['close'])/row['pre_close']>0.10:
                    buy_price = row['close']
                elif row['pct_chg']>9.9:
                    continue
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.03
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['pct_chg']>9.9 or row['pct_chg']<-9.9:
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

def first_yin2(series):
    '''收绿，第二天竞价买'''
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    next_buy = 0
    #如果是20点封顶
    if pct > 19.9:
        for index, row in df.iterrows():
            #print(row['trade_date'])
            if next_buy ==1:
                buy_price = row['open']
                next_buy = 0
                continue
                
            if buy_price == 0 :  #尚未买入
                if (row['open'] - row['close'])/row['pre_close']>0.20:
                    next_buy =1
                elif row['pct_chg']>19.9:
                    continue
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.06  
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['pct_chg']>19.9 or row['pct_chg']<-19.9:
                    #print("涨停或跌停 继续算下一天")
                    continue
                else:
                    sell_price = row['close'] 
                    #print("尾盘卖出")
                    break
    else:
        #10个点封顶
        for index, row in df.iterrows():
            #print(row['trade_date'])
            if next_buy ==1:
                buy_price = row['open']
                next_buy = 0
                continue
            if buy_price == 0 :  #尚未买入
                if (row['open'] - row['close'])/row['pre_close']>0.10:
                    next_buy =1
                elif row['pct_chg']>9.9:
                    continue
                else:
                    return np.nan
            else:#已买入，卖出
                loss_stop = row['open'] - row['pre_close']*0.03
                if row['low']< loss_stop:
                    #print("触发-3卖出")
                    sell_price = loss_stop
                    break
                elif row['pct_chg']>9.9 or row['pct_chg']<-9.9:
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

def yin2_open(series):
    '''收绿，第二天竞价买'''
    df = pro.daily(ts_code=series['ts_code'], start_date=series['trade_date']) 
    df = df[:-1]
    df = df.tail(15)
    df.sort_values(by='trade_date',ascending=True,inplace=True)
   # print('\n')
    print(series['trade_date'],end=',')
    pct = series['pct_chg']
    buy_price = 0
    sell_price = 0
    next_buy = 0
    #如果是20点封顶
    if pct > 19.9:
        for index, row in df.iterrows():
            #print(row['trade_date'])
            if next_buy ==1:
                buy_price = row['open']
                rate = (row['open'] - row['pre_close'])/row['pre_close']*100
                return round(rate,2)
             
                
            if buy_price == 0 :  #尚未买入
                if (row['open'] - row['close'])/row['pre_close']>0.20:
                    next_buy =1
                elif row['pct_chg']>19.9:
                    continue
                else:
                    return np.nan
             
    else:
        #10个点封顶
        for index, row in df.iterrows():
            #print(row['trade_date'])
            if next_buy ==1:
                buy_price = row['open']
                rate = (row['open'] - row['pre_close'])/row['pre_close']*100
                return round(rate,2)
            if buy_price == 0 :  #尚未买入
                if (row['open'] - row['close'])/row['pre_close']>0.10:
                    next_buy =1
                elif row['pct_chg']>9.9:
                    continue
                else:
                    return np.nan
             
    


# In[86]:


engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')
sql = """
    select * from work;
"""
df = pd.read_sql(sql, engine)


# In[62]:


#df = df.loc[df['limit_amount']>3,:]


# In[87]:


temp_df = pd.DataFrame(columns=['ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol'                                ,'amount','limit_amount','open_profit'])


# In[64]:


#连续涨停的唯一，保留第一板
flag = ""
for index,row in df.iterrows():
    if row['ts_code'] != flag:
        temp_df = temp_df.append(row)
        flag = row['ts_code']
    else:
        continue


# In[88]:


#连续涨停的唯一，只保留最高板
temp_df.append(df.iloc[0,:])
last_row = df.iloc[0,:]
for index,row in df.iterrows():
    if row['ts_code'] == last_row['ts_code']:
        continue
    else:
        temp_df = temp_df.append(last_row)
        last_row = row


# In[89]:


temp_df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[90]:


temp_df['first_yin']= temp_df.apply(first_yin,axis=1)


# In[91]:


temp_df['first_yin'].sum()


# In[93]:


t2 = temp_df.loc[temp_df['first_yin']>-100,:]


# In[97]:


t2.loc[t2['amount']>10000,:].first_yin.sum()


# In[99]:


t2


# In[ ]:





# In[ ]:





# In[47]:





# In[ ]:





# In[100]:


temp_df['first_yin2']= temp_df.apply(first_yin2,axis=1)


# In[106]:


df2 = temp_df.loc[temp_df['first_yin2']>-50,:]


# In[107]:


df2.first_yin2.sum()


# In[108]:


df2 = df2.loc[df2['amount']>10000,:] 


# In[109]:


df2 = df2.loc[df2['limit_amount']>2,:]


# In[110]:


df2.first_yin2.sum()


# In[ ]:





# In[ ]:





# In[111]:


temp_df['yin2_open'] = temp_df.apply(yin2_open,axis =1)


# In[112]:


temp_df['first_yin2'].groupby(pd.cut(temp_df['yin2_open'],list(range(-11,12,1)))).agg(["count","sum"])


# In[113]:


temp_df.loc[temp_df['first_yin2']>-100,:]


# In[120]:


df2 = temp_df.loc[temp_df['first_yin2']>-50,:]


# In[121]:


df2.first_yin2.sum()


# In[122]:


df2 = df2.loc[df2['amount']>10000,:] 


# In[126]:


df2 = df2.loc[df2['limit_amount']>4,:]


# In[127]:


df2.first_yin2.sum()


# In[128]:


df2['first_yin2'].groupby(pd.cut(df2['yin2_open'],list(range(-11,12,1)))).agg(["count","sum"])


# In[129]:


df2


# In[ ]:





# In[44]:


def draw_result(df):
    df['cumsum'] = df['first_yin'].cumsum()
    top = df['cumsum'].max()+20
    bottom = df['cumsum'].min()-20
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(30,30),dpi=100)
    date = df['trade_date'].tolist()
    date = [str(i) for i in date]
    result = df['cumsum'].tolist()
    df.reset_index(inplace=True)
    plt.plot(df.index.values,result)
    plt.xticks(np.arange(1,len(df),30),date[::30],rotation=45,size=14)
    plt.yticks(np.arange(bottom,top,10),size=16)
    fig = plt.gcf()
    fig.set_size_inches(60,20)
    plt.grid(ls='--',c='darkblue')
    plt.savefig('result.jpg')
    plt.show()


# In[29]:


draw_result(temp_df)


# In[46]:


temp_df.loc[temp_df['first_yin2']<-10,:]


# In[54]:


temp_df.loc[(temp_df['first_yin2']>-200)&(temp_df['limit_amount']>6),:]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




