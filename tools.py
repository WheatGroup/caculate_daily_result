#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    tools.py
@time:    2018/4/17 14:29
"""
import pandas as pd
import tushare as ts
from config import QueryDbServer, ExecuDbServer


result = ts.trade_cal()
df = result[(result.calendarDate >= '2018-01-01') & (result.isOpen == 1)]
df2 = result[(result.calendarDate >= '2017-12-01') & (result.calendarDate <= '2018-01-01') & (result.isOpen == 1)] .iloc[-1:].append(df)
trading_day_df = df2.reset_index(drop=True)[['calendarDate']]


def get_last_trading_day(year, month):
    # tm = pd.Timestamp(year=i.year, month=i.month, day=i.day)
    if month == 12:
        year = year + 1
        month = 1
    else:
        month = month + 1
    next_month_1st_day = pd.Timestamp(year=year, month=month, day=1)
    df = trading_day_df[trading_day_df.calendarDate < str(next_month_1st_day)[0:10]].iloc[-1:]
    return pd.Timestamp(df.iloc[-1]['calendarDate'])


def code2wind(code: str):
    if code[0] == '6':
        code = code + '.SH'
    else:
        code = code + '.SZ'
    return code


def get_pro_trading_day(TradingDay: str):
    index = trading_day_df[trading_day_df.calendarDate==TradingDay].index
    pro_trading_day = trading_day_df.iloc[index-1]['calendarDate'].iloc[0]
    return pro_trading_day


def get_today_code(TradingDay: str):
    select_sql = "select ts_code from stock_basic where trade_date = '%s';" % TradingDay
    code_df = QueryDbServer.query(select_sql)
    code_list = code_df['ts_code'].tolist()
    return code_list