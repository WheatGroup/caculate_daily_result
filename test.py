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
from config import QueryDbServer

today = '2018-10-30'

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
    select_sql = "select *from daily_result_detail where date = '%s'" %(today)
    tick_daily_df = QueryDbServer.query(select_sql)
    tick_daily_df['close_price'] = tick_daily_df['code'].apply(get_close_price)
    tick_daily_df['ten_price'] = tick_daily_df['code'].apply(get_ten_price)
    print(tick_daily_df)




