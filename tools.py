#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  quantpy
@file:    tools.py
@time:    2018/4/17 14:29
"""
import pandas as pd
import numpy as np

from config import mysql_engine, QueryDbServer


from datetime import datetime, date, timedelta

import tushare as ts
import time, traceback


result = ts.trade_cal()
df = result[(result.calendarDate >= '2018-01-01') & (result.isOpen == 1)]
df2 = result[(result.calendarDate >= '2017-12-01') & (result.calendarDate <= '2018-01-01') & (result.isOpen == 1)] .iloc[-1:].append(df)
trading_day_df = df2.reset_index(drop=True)[['calendarDate']]

def get_3(day):
    ###  获取今日涨停股票的数量
    sql = "select count(*) as num from daily_result_detail where date = '%s' and close_is_raiselimit = 1;" % day
    num_df = pd.read_sql(sql, mysql_engine)
    print(num_df['num'][0])
    return num_df['num'][0]

def get_4(day):
    #当天的非一字板，不包括 st
    sql = "select count(*) as num from daily_result_detail where date = '%s' and close_is_one = 0 and close_is_raiselimit = 1;" % day
    num_df = pd.read_sql(sql, mysql_engine)
    print(num_df['num'][0])
    return num_df['num'][0]

def get_5(day):
    return get_ten_info(day, 0)

    # #十点前上板的非一字涨停板,不包括 st
    # sql = "select count(*) as num from daily_result_detail where date = '%s' and ten_is_one = 0 and ten_is_raiselimit = 1;" % day
    # num_df = pd.read_sql(sql, mysql_engine)
    # print(num_df['num'][0])
    # return num_df['num'][0]

def get_6(day):
    #手动填写文字
    return "null"

def get_7(day):
    #手动填写文字
    return "null"

def get_8(day):
    #上一个的全部涨停板（上一个交易日）
    sql = "select count(*) as num from daily_result_detail where date = '%s';" % day
    num_df = pd.read_sql(sql, mysql_engine)
    print(num_df['num'][0])
    return num_df['num'][0]

def get_9(day):
    #上一个交易日非一字数量（上一个交易日）
    sql = "select count(*) as num from daily_result_detail where date = '%s' and close_is_one = 0 and close_is_raiselimit = 1;" % day
    num_df = pd.read_sql(sql, mysql_engine)
    print(num_df['num'][0])
    return num_df['num'][0]

def get_10(day):
    #上一个交易日
    pre_today = get_pro_trading_day(day)
    return pre_today

def get_11(day):
    #高开率=(昨天非一字涨停板在今天是高开的数量)/昨天非一字涨停板的数量
    pre_today = get_pro_trading_day(day)
    fenmu = get_9(pre_today)
    pre_limit_up_info = get_limit_up_detail(0, pre_today, 0)
    fenzi = 0
    # for code_info in pre_limit_up_info:
    for i, row in pre_limit_up_info.iterrows():
        if is_gaokai(row, day):
            fenzi = fenzi+1
            print(fenzi)
    gaokai_chance = round(fenzi/fenmu, 2)
    gaokai_chance = str(gaokai_chance * 100) + '%'
    return gaokai_chance

def get_18(day):
    return get_10(day)

def get_19(day):
    #成功率=收盘价格上涨/昨日非一字板的数量
    pre_today = get_pro_trading_day(day)
    fenmu = get_9(pre_today)
    pre_limit_up_info = get_limit_up_detail(0, pre_today, 0)
    fenzi = 0
    for i, row in pre_limit_up_info.iterrows():
        print(i)
        if is_shangzhang(0, row, day):
            fenzi = fenzi + 1

    success_rate = round(fenzi/fenmu, 2)
    success_rate = str(success_rate * 100) + '%'
    return success_rate

def get_25(day):
    return get_5(day)

def get_26(day):
    return get_10(day)

def get_27(day):
    pre_today = get_pro_trading_day(day)
    fenmu = get_ten_info(pre_today, 1)
    #获取昨天10点之前涨停股票信息
    pre_limit_up_info = get_limit_up_detail(1, pre_today, 0)

    fenzi = 0
    for i, row in pre_limit_up_info.iterrows():
        print(i)
        if is_gaokai_sucess(1, row, day):
            fenzi = fenzi + 1

    gaokai_chance = round(fenzi/fenmu, 2)
    gaokai_chance = str(gaokai_chance * 100) + '%'
    return gaokai_chance

def get_28(day):
    #成功率=昨日10:00涨停的收盘价格上涨/昨日10:00之前非一字板的数量
    pre_today = get_pro_trading_day(day)
    fenmu = get_ten_info(pre_today, 1)
    pre_limit_up_info = get_limit_up_detail(1, pre_today, 2)
    fenzi = 0
    for i, row in pre_limit_up_info.iterrows():
        print(i)
        if is_shangzhang(1, row, day):
            fenzi = fenzi + 1
    success_rate = round(fenzi/fenmu, 2)
    success_rate = str(success_rate*100) + '%'
    return success_rate


def get_elements():
    elements_list = []
    today = datetime.now().strftime('%Y-%m-%d')
    # today = '2019-04-26'
    pre_today = get_pro_trading_day(today)
    element1 = datetime.now().strftime('%m/%d')
    element2 = datetime.now().strftime('%m') + "月" + datetime.now().strftime('%d') + "日"

    elements_list.extend([today, element1, element2])

    open_shangzhang_num, close_shangzhang_num = shangzhang_rate(today)

    element_3 = get_3(today)
    element_4 = get_4(today)
    element_5 = get_5(today)
    element_6 = get_6(today)
    element_7 = get_7(today)
    element_8 = get_8(pre_today)
    element_9 = get_9(pre_today)
    element_10 = get_10(today)
    element_11 = get_11(today)
    element_12 = open_shangzhang_num[0]
    element_13 = open_shangzhang_num[1]
    element_14 = open_shangzhang_num[2]
    element_15 = open_shangzhang_num[3]
    element_16 = open_shangzhang_num[4]
    element_17 = open_shangzhang_num[5]
    element_18 = get_18(today)
    element_19 = get_19(today)
    element_20 = close_shangzhang_num[0]
    element_21 = close_shangzhang_num[1]
    element_22 = close_shangzhang_num[2]
    element_23 = close_shangzhang_num[3]
    element_24 = close_shangzhang_num[4]
    element_25 = get_25(today)
    element_26 = get_26(today)
    element_27 = get_27(today)
    element_28 = get_28(today)


    elements_list.extend([element_3,element_4,element_5,element_6,element_7,element_8,element_9,element_10,element_11,\
                          element_12,element_13,element_14,element_15,element_16,element_17,element_18,element_19,\
                          element_20,element_21,element_22,element_23,element_24,element_25,element_26,element_27,element_28])

    return elements_list

def save_element():
    element_list = get_elements()
    sql_inset_daily_28 = "insert into daily_28(trade_date, element1, element2, element3, element4, element5, element6, element7, element8, element9, element10, element11, element12, element13, element14, \
                        element15, element16, element17, element18, element19, element20, element21, element22, element23, element24, element25, element26, element27, element28) VALUES( \
                        '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" \
            %(element_list[0], element_list[1], element_list[2], element_list[3],element_list[4], element_list[5], element_list[6], element_list[7],\
              element_list[8], element_list[9], element_list[10], element_list[11],element_list[12], element_list[13], element_list[14], element_list[15], \
              element_list[16], element_list[17], element_list[18], element_list[19], element_list[20], element_list[21], element_list[22], element_list[23], \
              element_list[24], element_list[25], element_list[26], element_list[27], element_list[28]);
    mysql_engine.execute(sql_inset_daily_28)
    # pd.read_sql(sql_inset_daily_28, mysql_engine)

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

def get_limit_up_detail(is_ten, day, is_one):
    #获取当天涨停股票的内容,包括 股票代码code
    if 0 == is_ten and 0 == is_one:
        #当天非一字涨停板的信息
        sql = "select code,close_price,ten_price from daily_result_detail where date = '%s'  and close_is_one = 0 and close_is_raiselimit = 1;" % day
    elif 0 == is_ten and 1 == is_one:
        #当天一字板的信息
        sql = "select code,close_price,ten_price from daily_result_detail where date = '%s' and close_is_one = 1 and close_is_raiselimit = 1;" % day
    elif 0 == is_ten and 2 == is_one:
        # 当天涨停板的信息（一字板+非一字板）  ？？？？这个地方应该用一个枚举值
        sql = "select code,close_price,ten_price from daily_result_detail where date = '%s' and close_is_raiselimit = 1;" % day
    else:
        sql = "select code,close_price,ten_price from daily_result_detail where date = '%s' and ten_is_raiselimit = 1;" % day

    detail_info = pd.read_sql(sql, mysql_engine)
    return detail_info

def is_shangzhang(is_ten, code_info, day):
    code = code_info['code']
    tmp = get_code_info(is_ten, code, day)
    if 0 == is_ten:
        if (code_info['close_price'] < tmp['now'][0]):
            return True
        else:
            return False
    else:
        if (code_info['ten_price'] < tmp['now'][0]):
            return True
        else:
            return False

def get_ten_info(day, is_one):
    if 0 == is_one:
        # 十点前上板的非一字涨停板,不包括 st
        sql = "select count(*) as num from daily_result_detail where date = '%s' and ten_is_one = 0 and ten_is_raiselimit = 1;" % day
    else:
        #十点前上板的涨停，一字板+非一字板
        sql = "select count(*) as num from daily_result_detail where date = '%s' and ten_is_raiselimit = 1;" % day

    num_df = pd.read_sql(sql, mysql_engine)
    print(num_df['num'][0])
    return num_df['num'][0]

def is_gaokai(row, day):
    return is_gaokai_sucess(0, row, day)

def is_sucess(code_info, day):
    return is_gaokai_sucess(0, code_info, day)

def is_gaokai_sucess(is_ten, row, day):
    sql = "select * from `%s` where code = '%s' and trade_time > '09:30:00';" %(day, row['code'])
    tmp = pd.read_sql(sql, mysql_engine)
    if tmp.empty:
        return False
    else:
        if (0 == is_ten):
            if (row['close_price'] < tmp['open'][0]):
                print((row['code']), (row['close_price']), (tmp['open'][0]))
                return True
            else:
                return False
        else:
            if (row['ten_price'] < tmp['open'][0]):
                return True
            else:
                return False

def get_lianxu_limitup(day):
    sql = "select *from daily_result_detail where date = '%s' and num_raiselimit > 1;" % (day)
    detail_info = pd.read_sql(sql, mysql_engine)
    if detail_info.empty:
        return 0
    else:
        return detail_info.shape[0]

def get_code_info(is_ten, code, day):
    if 0 == is_ten:
        #15：00之后的now 就是收盘价了
        sql = "select * from `%s` where code = '%s' and query_time > '15:00:00' limit 1;" %(day, code)
    elif 1 == is_ten:
        #10：00的价格
        sql = "select * from `%s` where code = '%s' and query_time > '09:59:59' limit 1;" %(day, code)
    else:
        #9：29开盘价
        sql = "select * from `%s` where code = '%s' and query_time > '09:29:59' limit 1;" %(day, code)
    tmp = pd.read_sql(sql, mysql_engine)
    return tmp

def shangzhang_rate(day):
    #昨日涨停今日开盘,一字开盘、(-&, -2%),[-2%,0),[0,2%),[2%,5%),[5%,+$) 左闭右开的个数
    rate1_num = []
    #昨日涨停今日收盘,[连板/上涨5%+/0~5%/下跌/停盘]
    rate2_num = []
    rate0 = rate1 = rate2 = rate3 = rate4 = rate5 = 0
    rate6 = rate7 = rate8 = rate9 = rate10 = 0

    pre_today = get_pro_trading_day(day)
    limit_up_info = get_limit_up_detail(0, pre_today, 2)
    for i, row in limit_up_info.iterrows():
        # if row['code'] == '600156':
        this_code_today_info = get_code_info(0, row['code'], day)
        try:
            #开盘价的表现
            rate1_tmp_series = (this_code_today_info['open'].values[0] - row['close_price']) / row['close_price']
            rate1_tmp = rate1_tmp_series
            print(i, '===', rate1_tmp, "====", row['code'], "====", this_code_today_info['open'].values[0], "====", row['close_price'])
            # if (this_code_today_info['bid1'][0] == this_code_today_info['now'][0]):
            if (this_code_today_info['bid1'][0] == this_code_today_info['open'][0]):
                #一字开盘
                rate0 = rate0+1
            elif(rate1_tmp < -0.02):
                rate1 = rate1+1
            elif(rate1_tmp < 0):
                rate2 = rate2+1
            elif(rate1_tmp < 0.02):
                rate3 = rate3+1
            elif(rate1_tmp < 0.05):
                rate4 = rate4+1
            else:
                rate5 = rate5+1
            #收盘价的表现
            rate2_tmp_series = (this_code_today_info['now'] - row['close_price']) / row['close_price']
            rate2_tmp = rate2_tmp_series[0]
            if (0 == this_code_today_info['turnover'][0]):
                rate10 = rate10 + 1
            elif (rate2_tmp < 0):
                rate7 = rate7 + 1
            elif (rate2_tmp < 0.05):
                rate8 = rate8 + 1
            else:
                rate9 = rate9 + 1
        except Exception as e:
            traceback.print_exc()
    ###  rate6 计算的是 今日连板的数量 也就是从daily_result_detail中选出大于2的
    rate6 = get_lianxu_limitup(day)
    rate1_num.extend([rate0, rate1, rate2, rate3, rate4, rate5])
    rate2_num.extend([rate6, rate7, rate8, rate9, rate10])
    return rate1_num, rate2_num

def get_pro_trading_day(TradingDay: str):
    index = trading_day_df[trading_day_df.calendarDate==TradingDay].index
    pro_trading_day = trading_day_df.iloc[index-1]['calendarDate'].iloc[0]
    print(pro_trading_day)
    return pro_trading_day

def time2str(tradeTime):
    hour = str(tradeTime.timetuple().tm_hour)
    minute = str(tradeTime.timetuple().tm_min)
    second = str(tradeTime.timetuple().tm_sec)
    return (hour+minute+second)[0:4]

def get_today_code_info(day, code):
    #根据股票代码，返回股票名称，打板时间，第几版
    sql = "select * from `%s` where code = '%s' limit 1;" %(day, code)
    tmp = pd.read_sql(sql, mysql_engine)
    print(day, code)
    code_name = tmp['name'][0]
    #打板时间和第几版
    sql = "select time_raiselimit,num_raiselimit from daily_result_detail where date = '%s' and code = '%s';" % (day, code)
    tmp = pd.read_sql(sql, mysql_engine)
    time_raiselimit = tmp['time_raiselimit'].values[0]
    num_raiselimit = tmp['num_raiselimit'].values[0]
    return {'code':code, 'name':code_name, 'time':time_raiselimit, 'frequency':str(num_raiselimit)}

def get_today_code(TradingDay: str):
    select_sql = "select ts_code from stock_basic where trade_date = '%s';" % TradingDay
    code_df = QueryDbServer.query(select_sql)
    code_list = code_df['ts_code'].tolist()
    return code_list

if __name__ == '__main__':
    # day = '2019-04-12'
    # code = '603885'
    # dict = get_today_code_info(day, code)
    # get_28(day)
    get_elements()
