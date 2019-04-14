#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  ALIENWARE
@email:   wangrui0810@gmail.com
@file:    easy_caculate.py
@time:    2018/10/28 15:15
"""
from datetime import date, datetime, time
import pandas as pd
import numpy as np
from config import QueryDbServer, mysql_engine
import tushare as ts
from tools import save_element


today = date.today().strftime('%Y-%m-%d')
today = '2019-04-11'
table_name =  today
result = ts.trade_cal()
df = result[(result.calendarDate >= '2018-01-01') & (result.isOpen == 1)]
df2 = result[(result.calendarDate >= '2017-12-01') & (result.calendarDate <= '2018-01-01') & (result.isOpen == 1)] .iloc[-1:].append(df)
trading_day_df = df2.reset_index(drop=True)[['calendarDate']]



def is_in(code: str, code_list: list):
    if code in code_list:
        return True
    else:
        return False

def get_pro_trading_day(TradingDay: str):
    index = trading_day_df[trading_day_df.calendarDate==TradingDay].index
    pro_trading_day = trading_day_df.iloc[index-1]['calendarDate'].iloc[0]
    print(pro_trading_day)
    return pro_trading_day


def caculate_limitup_time(code: str):
    selectsql = "select *from `%s` where code = '%s' and trade_date = '%s' order by trade_time;"%(table_name, code, today)
    code_df = QueryDbServer.query(selectsql)
    code_df['limit_up'] = round(code_df['yst_close'] * 1.1, 2)
    limit_up_time = ''
    for index, rows in code_df.iterrows():
        if rows['now'] == rows['limit_up']:
            limit_up_time = rows['trade_time']
            break
    return limit_up_time


def get_close_price(code: str):
    selectsql = "select now from `%s` where code = '%s' and trade_date = '%s' and trade_time > '14:59:59' order by trade_time limit 1;"\
    %(table_name, code, today)
    close_df = QueryDbServer.query(selectsql)
    return close_df['now'][0]

def get_open_price(code: str):
    selectsql = "select open from `%s` where code = '%s' and trade_date = '%s' and trade_time > '14:59:59' order by trade_time limit 1;"\
    %(table_name, code, today)
    open_df = QueryDbServer.query(selectsql)
    return open_df['open'][0]


def get_ten_price(code: str):
    selectsql = "select now from `%s` where code = '%s' and trade_date = '%s' and trade_time > '09:59:59' order by trade_time limit 1;"\
    %(table_name, code, today)
    ten_df = QueryDbServer.query(selectsql)
    return ten_df['now'][0]

def get_num_raiselimit(code:str):
    pre_day = get_pro_trading_day(today)
    sql = "select num_raiselimit from daily_result_detail where code = '%s' and date = '%s' and close_is_raiselimit = 1;" %(code, pre_day)
    ten_df = QueryDbServer.query(sql)
    if ten_df.empty:
        return 1
    else:
        return int(ten_df['num_raiselimit'][0]) + 1


if __name__ == "__main__":
    ### 读取当天所有的涨停过的股票
    fh = open('limit_up_code.txt', 'r', encoding='utf-8')
    codes_str = fh.read()
    fh.close()
    symbol = codes_str.split(',')
    ### 每天预先创建当前的表
    # mysql_engine.execute("DROP TABLE IF EXISTS `2019-03-18`;")

    # 需要先去
    #  date symbol ten_is_raiselimit ten_is_one close_is_raiselimit raisenum_one symbol
    # 先取出当天的所有交易时间点
    time_sql = "SELECT DISTINCT query_time from `%s`;" % (table_name)
    query_time_df = QueryDbServer.query(time_sql)
    ten_query_time = query_time_df[(query_time_df.query_time > "10:00:00")&(query_time_df.query_time < "10:01:00")]['query_time'].tolist()[-1]
    # 再者需要确认的是 是否一个query 对应的trade_time是同一个
    #  代码空缺
    #
    ten_query_time = str(ten_query_time)[-8:]
    selectsql = "select *from `%s` where query_time = '%s' and name not like '%%%%%s%%%%' and \
    name not like '%%%%%s%%%%' and trade_date = '%s';" %(table_name, ten_query_time, 'st', 'ST', today)
    ten_code_df = QueryDbServer.query(selectsql)
    ten_code_df['limit_up'] = round(ten_code_df['yst_close'] * 1.1, 2)
    # 除去代码中带有st的股票
    ten_is_raiselimit_df = ten_code_df[ten_code_df.limit_up == ten_code_df.bid1]
    # print(ten_is_raiselimit_df)
    ten_is_one_df = ten_code_df[(ten_code_df.high == ten_code_df.low) & (ten_code_df.open == ten_code_df.high) & (ten_code_df.open == ten_code_df.limit_up)]
    # print(ten_is_one_df)


##################################################################################
    # close_query_time = query_time_df[(query_time_df.query_time > "15:00:00") & (query_time_df.query_time < "15:01:00")][
    #     'query_time'].tolist()[-1]
    close_query_time = query_time_df['query_time'].tolist()[-1]
    # 再者需要确认的是 是否一个query 对应的trade_time是同一个
    #  代码空缺
    #
    close_query_time = str(close_query_time)[-8:]
    selectsql = "select *from `%s` where query_time = '%s' and name not like '%%%%%s%%%%' and \
        name not like '%%%%%s%%%%' and trade_date = '%s';" % (table_name, close_query_time, 'st', 'ST', today)
    close_code_df = QueryDbServer.query(selectsql)
    close_code_df['limit_up'] = round(close_code_df['yst_close'] * 1.1, 2)
    # 除去代码中带有st的股票
    date = today
    close_is_raiselimit_df = close_code_df[close_code_df.limit_up == close_code_df.bid1]
    # print(close_is_raiselimit_df)
    close_is_one_df = close_code_df[(close_code_df.high == close_code_df.low) & (close_code_df.open == close_code_df.high) & (
    close_code_df.open == close_code_df.limit_up)]
    # print(close_is_one_df)


    limit_up_df = pd.DataFrame(columns=['date', 'code', 'ten_is_raiselimit', 'ten_is_one', 'close_is_raiselimit', \
                          'close_is_one', 'time_raiselimit', 'num_raiselimit'])
    limit_up_df['code'] = symbol
    limit_up_df['date'] = today

    ten_is_raiselimit_list = ten_is_raiselimit_df['code'].tolist()
    ten_is_one_df_list = ten_is_one_df['code'].tolist()
    close_is_raiselimit_list = close_is_raiselimit_df['code'].tolist()
    close_is_one_list = close_is_one_df['code'].tolist()

    limit_up_df['ten_is_raiselimit'] = limit_up_df['code'].apply(lambda x: True if x in ten_is_raiselimit_list else False)
    limit_up_df['ten_is_one'] = limit_up_df['code'].apply(lambda x: True if x in ten_is_one_df_list else False)
    limit_up_df['close_is_raiselimit'] = limit_up_df['code'].apply(lambda x: True if x in close_is_raiselimit_list else False)
    limit_up_df['close_is_one'] = limit_up_df['code'].apply(lambda x: True if x in close_is_one_list else False)
    limit_up_df['time_raiselimit'] = limit_up_df['code'].apply(caculate_limitup_time)


    limit_up_df['close_price'] = limit_up_df['code'].apply(get_close_price)
    limit_up_df['ten_price'] = limit_up_df['code'].apply(get_ten_price)
    limit_up_df['open_price'] = limit_up_df['code'].apply(get_open_price)
    limit_up_df['num_raiselimit'] = limit_up_df['code'].apply(get_num_raiselimit)

    limit_up_df.to_sql('daily_result_detail', QueryDbServer.engine, index=False, if_exists='append')
    #只保留9:25~9:35 和 9:55~10:05 和 14:55~15:05,节省空间，提高后续程序查表时间
    sqldelete = "delete from `%s` where (query_time < '09:25:00') or (query_time > '09:35:00' and query_time < '09:55:00') or (query_time > '10:05:00' and query_time < '14:55:00');" %(table_name)
    mysql_engine.execute(sqldelete)
    save_element()


