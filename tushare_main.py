#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1
@author:  ALIENWARE
@email:   wangrui0810@gmail.com
@file:    main.py.py
@time:    2018/10/18 16:50
"""

import tushare as ts
from tools import *
from config import *
from datetime import datetime, date
from irm_logger import Logger
import time
logger = Logger(__file__, level=10)

today = date.today().strftime('%Y-%m-%d')


if __name__ == "__main__":
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
        # data = pro.query('stock_basic', exchange_id='', fields='symbol')
        stock_list = data['symbol'].tolist()







'''
    done_df_list = []
    undone_code_list = []
    for code in stock_list:
        code = code[0:6]
        print('\n')
        print(code)
        tick_daily = ts.get_today_ticks(code)
        if tick_daily.empty or type(tick_daily) != pd.DataFrame:
            undone_code_list.append(code)
        tick_daily['trade_date'] = today
        tick_daily['code'] = code
        tick_daily.rename(columns={'time': 'trade_time'}, inplace=True)
        tick_daily = tick_daily[tick_daily.trade_time < '10:00:01']
        tick_daily = tick_daily.reset_index(drop=True)
        tick_daily.to_sql('ten_tick_daily', con=QueryDbServer.engine, index=False, if_exists='append')
        time.sleep(20)
    undone_code = undone_code_list.join(',')
    logger.warn(undone_code)

'''