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
import pandas as pd
logger = Logger(__file__, level=10)
import timeit

today = date.today().strftime('%Y-%m-%d')
table_name = today

def is_limit_up(now, yst_close):
    limit_up = round(yst_close * 1.1, 2)
    now = round(now, 2)
    if now == limit_up:
        return 1
    else:
        return 0


if __name__ == "__main__":
    print("engine start!!")
    pro = ts.pro_api('d064869d86f5d030f0b287b98d132a8b4638ee7d41c1a2683c8fd9ba')
    sql = "CREATE TABLE `%s` (\
      `code` varchar(20) COLLATE utf8_general_mysql500_ci NOT NULL,\
      `yst_close` decimal(10,2) DEFAULT NULL,\
      `trade_date` date NOT NULL,\
      `high` double DEFAULT NULL,\
      `low` double DEFAULT NULL,\
      `name` varchar(255) COLLATE utf8_general_mysql500_ci DEFAULT NULL,\
      `now` double DEFAULT NULL,\
      `open` double DEFAULT NULL,\
      `trade_time` varchar(20) COLLATE utf8_general_mysql500_ci NOT NULL,\
      `turnover` bigint(20) DEFAULT NULL,\
      `volume` double DEFAULT NULL,\
      `chg` decimal(10,4) DEFAULT NULL,\
      `query_time` time NOT NULL,\
      `bid1` double DEFAULT NULL,\
      PRIMARY KEY (`query_time`,`trade_time`,`trade_date`,`code`),\
      UNIQUE KEY `unique_index` (`code`,`trade_date`,`trade_time`,`query_time`) USING BTREE,\
      KEY `date_time` (`trade_date`,`trade_time`) USING BTREE\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_mysql500_ci;"
    create_table_sql = sql %(table_name)
    #mysql_engine.execute(create_table_sql)
    # 定时抓取 每日市场信息
    stock_list = get_today_code(today)
    if len(stock_list) == 0:
        data = pro.query('stock_basic', exchange_id='', list_status='L', fields='symbol')
        stock_list = data['symbol'].tolist()
    quotation = easyquotation.use("sina")
    limit_up_codes = set()
    while(True):
        sleep(30)
        time_now = datetime.now().time()
        if (time_now > time(9, 25) and time_now < time(11, 30)) or (time_now > time(13, 00) and time_now < time(15,5)):
            data = quotation.stocks(stock_list)
            df = pd.DataFrame(data).T
            df = df[['date', 'time', 'close', 'open', 'high', 'low', 'now', 'name', 'bid1','volume', 'turnover']]
            df = df[df['name'].str.contains('^((?!ST).)*$')]
            df['query_time'] = datetime.now().strftime('%H:%M:%S')
            df.reset_index(inplace=True)
            df.rename(columns={'index':'code', 'date':'trade_date', 'time':'trade_time', 'close':'yst_close'}, inplace=True)
            df['is_limit_up'] = df.apply(lambda row: is_limit_up(row['high'], row['yst_close']),axis=1)
            limit_up_df = df[df.is_limit_up == 1]
            limit_up_codes.update(limit_up_df['code'].tolist())
            df.to_sql(table_name, con=QueryDbServer.engine, index=False, if_exists='append')
            # df.to_excel('tick_daily.xlsx', index=False, if_exists='append')
            print("数据入库完毕")
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(now)
