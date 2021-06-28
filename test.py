# -*- coding: utf-8

import tushare as ts
import Model_Tushare
import matplotlib.pyplot as plt
import pandas as pd

ts.set_token("e927776ce8870cdbff3aa17fb97071659c9f723578bcd83f901ee0e9")
pro = ts.pro_api()

df = pro.daily(ts_code='000001.SZ', start_date="20200101", end_date="20200110")
pd_data = df['close'].rolling(window=2).mean()
print(pd_data)
# 显示图表
df['close'].plot()

plt.show()


def stock_basic(is_hs="", list_status="", ts_code=""):
    """
    :param is_hs: 是否沪深港通标的，N否 H沪股通 S深股通
    :param list_status:上市状态 L上市 D退市 P暂停上市，默认是L
    :param ts_code:TS股票代码
    :return:pd数组
    """
    pass
