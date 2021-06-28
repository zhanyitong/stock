#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@function:   获取数据后写入数据库
@File    :   Stock_Install.py    
@Contact :   raogx.vip@hotmail.com
@License :   (C)Copyright 2017-2018, Liugroup-NLPR-CASIA

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/6/12 下午2:31   gxrao      1.0         None
'''

import Model_Tushare
import MySQL
import threading
import time


def clearing(number):
    if number != number:
        return 0
    else:
        return number


def threading_enumerate(number=100):
    while True:
        thread_num = len(threading.enumerate())
        if thread_num <= number:
            break
        time.sleep(0.5)


def stock_basic():
    '''
    获取挖动兔的全部A股，股票信息数据写入数据库
    :return:
    '''
    stock = Model_Tushare.stock_basic()
    for i in range(stock.shape[0]):
        threading_enumerate()
        ts_code = stock.loc[i]['ts_code']
        name = stock.loc[i]['name']
        area = stock.loc[i]['area']
        industry = stock.loc[i]['industry']
        market = stock.loc[i]['market']
        sql_insert = "INSERT INTO stock_basic(ts_code,name,area,industry,market) VALUES ('%s', '%s','%s','%s','%s')" % (
            ts_code, name, area, industry, market)
        t = threading.Thread(target=threading_install, args=(sql_insert,))
        t.start()


def stock_daily_input():
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    # 获取数据库里的所有开盘日期
    stock_day = mysql.get_all("SELECT * FROM stock_day")
    for day in stock_day:
        day = day[0]
        # 获取挖地兔的日线数据
        stock = Model_Tushare.stock_daily("", day)
        for i in range(stock.shape[0]):
            threading_enumerate(200)
            ts_code = stock.loc[i]['ts_code']
            trade_date = stock.loc[i]['trade_date']
            open = stock.loc[i]['open']
            high = stock.loc[i]['high']
            low = stock.loc[i]['low']
            close = stock.loc[i]['close']
            pre_close = stock.loc[i]['pre_close']
            change = stock.loc[i]['change']
            pct_chg = stock.loc[i]['pct_chg']
            vol = stock.loc[i]['vol']
            amount = stock.loc[i]['amount']
            # 写入日线的MySQL语句
            sql_insert = "INSERT INTO stock_daily(`ts_code`,`trade_date`,`open`,`high`,`low`,`close`,`pre_close`," \
                         "`change`,`pct_chg`,`vol`,`amount`) VALUES ('%s', '%s', '%.2f', '%.2f','%.2f','%.2f','%.2f'," \
                         "'%.2f','%.2f','%.2f','%.2f')" % (
                             ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount)
            t = threading.Thread(target=threading_install, args=(sql_insert,))
            t.start()
    mysql.close()


def day_input():
    '''
       获取挖动兔的股票开盘日期，写入数据库
    '''
    # 设置获取指定日期内的开盘日
    stock = Model_Tushare.get_trade_date("2020-01-01", "2021-06-12")
    for i in stock:
        # 线程限制
        threading_enumerate()
        # 替换获取的日期后取出-符号
        res = i.replace("-", "")
        # 写入数据库的sql
        sql_insert = "INSERT INTO stock_day(day) VALUES ('%s')" % (
            res)
        t = threading.Thread(target=threading_install, args=(sql_insert,))
        t.start()


def stock_daily_basic_input():
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    # 获取数据库里的所有开盘日期
    stock_day = mysql.get_all("SELECT * FROM stock_day")
    for day in stock_day:
        day = day[0]
        # 获取挖地兔的日线数据
        stock = Model_Tushare.get_daily_basic(day)
        for i in range(stock.shape[0]):
            threading_enumerate(100)
            ts_code = stock.loc[i]['ts_code']
            trade_date = stock.loc[i]['trade_date']
            close = clearing(stock.loc[i]['close'])
            turnover_rate = clearing(stock.loc[i]['turnover_rate'])
            turnover_rate_f = clearing(stock.loc[i]['turnover_rate_f'])
            volume_ratio = clearing(stock.loc[i]['volume_ratio'])
            pe = clearing(stock.loc[i]['pe'])
            pe_ttm = clearing(stock.loc[i]['pe_ttm'])
            pb = clearing(stock.loc[i]['pb'])
            ps = clearing(stock.loc[i]['ps'])
            ps_ttm = clearing(stock.loc[i]['ps_ttm'])
            dv_ratio = clearing(stock.loc[i]['dv_ratio'])
            dv_ttm = clearing(stock.loc[i]['dv_ttm'])
            total_share = clearing(stock.loc[i]['total_share'])
            float_share = clearing(stock.loc[i]['float_share'])
            free_share = clearing(stock.loc[i]['free_share'])
            total_mv = clearing(stock.loc[i]['total_mv'])
            circ_mv = clearing(stock.loc[i]['circ_mv'])

            # 写入日线的MySQL语句
            sql_insert = "INSERT INTO stock_daily_basic(ts_code, trade_date, close, turnover_rate, turnover_rate_f, " \
                         "volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, dv_ttm, total_share, float_share, " \
                         "free_share, total_mv, circ_mv) VALUES ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                         "'%s','%s','%s','%s','%s','%s','%s','%s')" % (
                             ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb,
                             ps, ps_ttm,
                             dv_ratio, dv_ttm, total_share, float_share, free_share, total_mv, circ_mv)
            t = threading.Thread(target=threading_install, args=(sql_insert,))
            t.start()
    mysql.close()


def stock_money_flow_input():
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    # 获取数据库里的所有开盘日期
    stock_day = mysql.get_all("SELECT * FROM stock_day")
    for day in stock_day:
        day = day[0]
        # 获取挖地兔的日线数据
        stock = Model_Tushare.get_stock_moneyflow(day)
        for i in range(stock.shape[0]):
            threading_enumerate(100)
            ts_code = stock.loc[i]['ts_code']
            trade_date = stock.loc[i]['trade_date']
            buy_sm_vol = clearing(stock.loc[i]['buy_sm_vol'])
            buy_sm_amount = clearing(stock.loc[i]['buy_sm_amount'])
            sell_sm_vol = clearing(stock.loc[i]['sell_sm_vol'])
            sell_sm_amount = clearing(stock.loc[i]['sell_sm_amount'])
            buy_md_vol = clearing(stock.loc[i]['buy_md_vol'])
            buy_md_amount = clearing(stock.loc[i]['buy_md_amount'])
            sell_md_vol = clearing(stock.loc[i]['sell_md_vol'])
            sell_md_amount = clearing(stock.loc[i]['sell_md_amount'])
            buy_lg_vol = clearing(stock.loc[i]['buy_lg_vol'])
            buy_lg_amount = clearing(stock.loc[i]['buy_lg_amount'])
            sell_lg_vol = clearing(stock.loc[i]['sell_lg_vol'])
            sell_lg_amount = clearing(stock.loc[i]['sell_lg_amount'])
            buy_elg_vol = clearing(stock.loc[i]['buy_elg_vol'])
            buy_elg_amount = clearing(stock.loc[i]['buy_elg_amount'])
            sell_elg_vol = clearing(stock.loc[i]['sell_elg_vol'])
            sell_elg_amount = clearing(stock.loc[i]['sell_elg_amount'])
            net_mf_vol = clearing(stock.loc[i]['net_mf_vol'])
            net_mf_amount = clearing(stock.loc[i]['net_mf_amount'])

            # 写入日线的MySQL语句

            sql_insert = "INSERT INTO stock_moneyflow(ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, " \
                         "sell_sm_amount, buy_md_vol, buy_md_amount, sell_md_vol, sell_md_amount, buy_lg_vol, " \
                         "buy_lg_amount, sell_lg_vol, sell_lg_amount, buy_elg_vol, buy_elg_amount, sell_elg_vol, " \
                         "sell_elg_amount, net_mf_vol, net_mf_amount) VALUES ('%s', '%s','%s','%s','%s','%s','%s'," \
                         "'%s','%s','%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
                             ts_code, trade_date, buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol,
                             buy_md_amount,
                             sell_md_vol, sell_md_amount, buy_lg_vol, buy_lg_amount, sell_lg_vol, sell_lg_amount,
                             buy_elg_vol,
                             buy_elg_amount, sell_elg_vol, sell_elg_amount, net_mf_vol, net_mf_amount)

            t = threading.Thread(target=threading_install, args=(sql_insert,))
            t.start()
    mysql.close()


def threading_install(sql_insert):
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    try:
        mysql.insert(sql_insert)
        mysql.commit()
    except Exception as e:
        try:
            e = str(e)
            if e.find("for key") > -1:
                pass
            else:
                time.sleep(10)
                threading_install(sql_insert)
        except Exception as ex:
            print(ex)
    finally:
        mysql.close()


stock_basic()
