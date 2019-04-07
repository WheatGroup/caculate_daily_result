#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  ALIENWARE
@email:   wangrui0810@gmail.com
@file:    test.py
@time:    2018/10/27 17:25
"""


from datetime import date, datetime, time
import pandas as pd
import numpy as np
from config import QueryDbServer, mysql_engine


def get_close_price(code: str):
    selectsql = "select now from tick_daily where code = '%s' and trade_date = '%s' and trade_time > '14:59:59' order by trade_time limit 1;"\
    %(code, today)
    close_df = QueryDbServer.query(selectsql)
    return close_df['now'][0]


def get_ten_price(code: str):
    selectsql = "select now from tick_daily where code = '%s' and trade_date = '%s' and trade_time > '09:59:59' order by trade_time limit 1;"\
    %(code, today)
    ten_df = QueryDbServer.query(selectsql)
    return ten_df['now'][0]


if __name__ == "__main__":
    select_sql = "select *from daily_result_detail where date = '2019-03-28';"
    df = QueryDbServer.query(select_sql)
    code_str = ",".join(df['code'].tolist())
    select_sql_tick_daily = "select distinct code, close from tick_daily where code in (%s) and trade_date = '2019-03-29';"%(code_str)
    # select_sql = "select *from tick_daily where trade_date = '%s' " %('2019-03-28')
    df2 = QueryDbServer.query(select_sql_tick_daily)
    all_df = df2.merge(df, on='code')
    all_df = all_df.drop(columns=['close_price'])
    all_df = all_df.rename(columns={'close': 'close_price'})
    mysql_engine.execute("delete from daily_result_detail where date = '2019-03-28'")
    all_df.to_sql('daily_result_detail', QueryDbServer.engine, index=False, if_exists='append')




