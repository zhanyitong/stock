#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
预测股票模块
@File    :   modeling.py    
@Contact :   raogx.vip@hotmail.com
@License :   (C)Copyright 2017-2018, Liugroup-NLPR-CASIA

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/6/14 下午2:10   gxrao      1.0         None
'''

import MySQL
import threading
import time
import model.Price as model_price


def calculation(a, b):
    a = float(a)
    b = float(b)
    c = (a - b) / b * 100
    return round(c, 2)


def threading_enumerate(number=100):
    while True:
        thread_num = len(threading.enumerate())
        if thread_num <= number:
            break
        time.sleep(0.5)


def model_build(accuracy, truncate=False):
    '''
    建立预测库模型
    :param accuracy: 精准度
    :param truncate: 是否清空预测库，数据库
    :return:
    '''
    mysql = MySQL.MysqlHelper()
    mysql.connect()

    # truncate 等于True 清空预出库
    if truncate:
        mysql.insert("truncate model_price")
        mysql.commit()
    # 获取stock_basic数据库里面的所有股票代码
    get_basic = mysql.get_all("SELECT (ts_code) FROM stock_basic")

    for i in get_basic:
        ts_code = i[0]
        print(ts_code)
        threading_enumerate(30)
        t = threading.Thread(target=model_price.init, args=(ts_code, accuracy))
        t.start()
    mysql.close()


def forecast(date):
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    res = mysql.get_all("SELECT * FROM stock_daily  WHERE trade_date='%s'" % date)
    mysql.close()
    return res


forecast("20200102")
