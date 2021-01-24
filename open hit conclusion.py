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


# In[63]:


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

def draw_result(df,name):
    top = df['cumsum'].max()+20
    bottom = df['cumsum'].min()-20
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.figure(figsize=(30,30),dpi=100)
    date = df['trade_date'].tolist()
    date = [str(i) for i in date]
    result = df['cumsum'].tolist()
    df.reset_index()
    plt.plot(df.index.values,result)
    plt.xticks(np.arange(1,len(df),30),date[::30],rotation=45,size=14)
    plt.yticks(np.arange(bottom,top,10),size=16)
    fig = plt.gcf()
    fig.set_size_inches(60,20)
    plt.grid(ls='--',c='darkblue')
    plt.savefig(name)
    plt.show()


# In[3]:


engine = create_engine('mysql+pymysql://root:901113@localhost:3306/stock')
sql = """
    select * from unique_table_all;
"""
df = pd.read_sql(sql, engine)


# In[4]:


#排除20涨幅的个股
df = df.loc[df['pct_chg']<10.1,:]


# In[5]:


df['open_profit'].groupby(pd.cut(df['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[6]:


"""
1.分析open -3:
竞价 a. -7到-10
     b.-7到-10 加上3-6     最大回撤-300 放弃 
     c.-7到-10 加上3-4    最大回撤-300 放弃 
     
d加上涨停数3板起变量
e加上股性变量：0，1......
"""


# In[7]:


df_a = df.loc[df['open_rate']<-7,:]
df_a['cumsum'] = df_a['open_profit'].cumsum()
df_a = df_a.reset_index(drop=True)


# In[9]:


draw_result(df_a,"df_open_a.jpg")


# In[10]:


df_b = df.loc[df['open_rate']<6,:]
df_b = df_b.loc[(df['open_rate']<-7)|(df['open_rate']>3),:]
df_b['cumsum'] = df_b['open_profit'].cumsum()
df_b = df_b.reset_index(drop=True)


# In[11]:


draw_result(df_b,"df_open_b.jpg")


# In[12]:


df_c = df.loc[df['open_rate']<4,:]
df_c = df_c.loc[(df['open_rate']<-7)|(df['open_rate']>3),:]
df_c['cumsum'] = df_c['open_profit'].cumsum()
df_c = df_b.reset_index(drop=True)


# In[13]:


draw_result(df_c,"df_open_c.jpg")


# In[14]:


df_e = df.loc[df['guxing']>=1,:]


# In[15]:


df_e['open_profit'].groupby(pd.cut(df_e['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[16]:


#前60天有2自然板，且竞价-3以下
df_e = df.loc[df['guxing']>=2,:]
df_e = df_e.loc[df_e['open_rate']<-3,:]
df_e['cumsum'] = df_e['open_profit'].cumsum()
df_e = df_e.reset_index(drop=True)


# In[17]:


draw_result(df_e,"df_open_e.jpg")


# In[18]:


df_e


# In[ ]:


"""
1.分析open end模式:
竞价 a. -7到-10        最大回撤-20 ，涨幅220左右
     b.-7到-10 加上3-5      最大回撤太大，弃
      
     
d加上涨停数3板起变量
e加上股性变量：0，1......
"""


# In[19]:


df['open_end_profit'].groupby(pd.cut(df['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[20]:



df_open_end = df.loc[df['open_rate']<-8,:]
df_open_end['cumsum'] = df_open_end['open_profit'].cumsum()
df_open_end = df_open_end.reset_index(drop=True)


# In[21]:


draw_result(df_open_end,"df_open_end.jpg")


# In[22]:


df_end_b = df.loc[df['open_rate']<5,:]
df_end_b = df_end_b.loc[(df_end_b['open_rate']<-8)|(df_end_b['open_rate']>3),:]
df_end_b['cumsum'] = df_end_b['open_profit'].cumsum()
df_end_b = df_end_b.reset_index(drop=True)


# In[23]:


draw_result(df_end_b,"df_end_b.jpg")


# In[24]:



df_open_end3 = df.loc[(df['open_rate']<-8)&(df['limit_amount']>2),:]
df_open_end3['cumsum'] = df_open_end3['open_profit'].cumsum()
df_open_end3 = df_open_end3.reset_index(drop=True)


# In[26]:


draw_result(df_open_end3,"df_open_end3.jpg")


# In[29]:


df_end_e = df.loc[df['guxing']>=2,:]
df_end_e = df_end_e.loc[df_end_e['open_rate']<-3,:]
df_end_e['cumsum'] = df_end_e['open_end_profit'].cumsum()
df_end_e = df_end_e.reset_index(drop=True)


# In[30]:


draw_result(df_end_e,"df_end_e.jpg")


# In[ ]:


"""
1.分析 hit 模式:
竞价 a. -7到-10        最大回撤-20 ，涨幅220左右
     b.-7到-10 加上3-5      最大回撤太大，弃
      
     
d加上涨停数3板起变量
e加上股性变量：0，1......
"""


# In[32]:


df['hit_profit'].groupby(pd.cut(df['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[52]:


df_hit = df.loc[(df['trade_date']>'20171231')&(df['trade_date']<"20190101"),:]
df_hit['cumsum'] = df_hit['hit_profit'].cumsum()
df_hit = df_hit.reset_index(drop=True)


# In[48]:


draw_result(df_hit,"df_hit.jpg")


# In[49]:


df_hit['hit_profit'].sum()


# In[53]:


df_hit = df_hit[["ts_code","trade_date","limit_amount","pct_chg","open_rate","hit_profit"]]


# In[54]:


df_hit


# In[64]:


df['hit_profit'] = df.apply(get_hit_profit,axis=1)


# In[69]:


df['hit_profit'].sum()


# In[71]:


df['hit_profit'].groupby(pd.cut(df['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[74]:


df.loc[df['hit_profit']!=np.nan,:]


# In[75]:


df = df[["ts_code","trade_date","limit_amount","pct_chg","open_rate","hit_profit"]]


# In[77]:


df = df.loc[df['hit_profit']>-200,:]


# In[79]:


df['cumsum'] = df['hit_profit'].cumsum()
df = df.reset_index(drop=True)


# In[80]:


draw_result(df,"df.jpg")


# In[84]:


test = df.loc[(df['trade_date']>'20171231')&(df['trade_date']<'20190101'),:]


# In[85]:


test['cumsum'] = test['hit_profit'].cumsum()
test = test.reset_index(drop=True)


# In[86]:


draw_result(test,"test.jpg")


# In[68]:


len(df_hit)


# In[88]:


test.head(40)


# In[90]:


df['hit_profit'].groupby(pd.cut(df['open_rate'],list(range(-11,12,1)))).agg(["count","sum"])


# In[91]:


df.loc[df['limit_amount']>2,:]['hit_profit'].sum()


# In[ ]:




