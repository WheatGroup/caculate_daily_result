#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  ALIENWARE
@email:   wangrui0810@gmail.com
@file:    main.py.py
@time:    2018/10/18 16:50
"""
from time import sleep
import tushare as ts
import easyquotation
from tools import *
from config import *
from datetime import datetime, date, time
from irm_logger import Logger
logger = Logger(__file__, level=10)
import timeit

today = date.today().strftime('%Y-%m-%d')


if __name__ == "__main__":

    print("engine start!!")
    pro = ts.pro_api('d064869d86f5d030f0b287b98d132a8b4638ee7d41c1a2683c8fd9ba')
    # 定时抓取 每日市场信息
    '''
    stock_basic = pro.query('stock_basic', exchange_id='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    if stock_basic.empty:
    raise ValueError('stock_basic 数据是空的')
        stock_basic['trade_date'] = today
    stock_basic = stock_basic.reset_index(drop=True)
        stock_basic.to_sql('stock_basic', con=QueryDbServer.engine, index=False, if_exists='append')
    '''
    stock_list = get_today_code(today)
    if len(stock_list) == 0:
        data = pro.query('stock_basic', exchange_id='', list_status='L', fields='symbol')
        stock_list = data['symbol'].tolist()
    quotation = easyquotation.use("sina")
    while(True):
        time_now = datetime.now().time()
        if(time_now > time(9, 10) and time_now < time(11, 30)) or (time_now > time(13, 0) and time_now > time(15, 10)):
            data = quotation.stocks(stock_list)
            df = pd.DataFrame(data).T
            df = df[['date', 'time', 'close', 'open', 'high', 'low', 'now', 'name', 'bid1','volume', 'turnover']]
            df = df[df['name'].str.contains('^((?!ST).)*$')]
            df['chg'] = (df['now'] - df['close'])/df['close']
            df['chg'] = df['chg'].apply(lambda x: "%.4f" % x)
            df['query_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.reset_index(inplace=True)
            df.rename(columns={'index':'code', 'date':'trade_date', 'time':'trade_time'}, inplace=True)
            # df.to_sql('tick_daily', con=QueryDbServer.engine, index=False, if_exists='append')
            df.to_excel('tick_daily.xlsx', index=False, if_exists='append')
            print("数据入库完毕")
            sleep(30)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(now)

