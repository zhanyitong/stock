'''
@Project ：stock 
@File    ：transaction.py
@IDE     ：PyCharm 
@Date    ：2021/6/22 7:19 下午 
@remarks ：交易模板
'''

import MySQL
import Deal


def get_price_forecast():
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    by_desc = mysql.get_one("SELECT * FROM model_price_forecast order by (reis) desc")
    mysql.close()
    return by_desc[0]


def transaction_main(predict_dt):
    # 更新持股天数

    sql_update_hold_days = 'update my_stock_pool w set w.hold_days = w.hold_days + 1'
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    mysql.insert(sql_update_hold_days)
    mysql.commit()
    deal = Deal.Deal(predict_dt)
    # 先卖出股票
    stock_pool_local = deal.stock_pool
    for stock in stock_pool_local:
        if stock == get_price_forecast():
            pass
    # 买入股票

    mysql.close()


transaction_main()
