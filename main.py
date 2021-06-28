# -*- coding: utf-8
# This is a sample Python script.

# import transaction_model.transaction as transaction
import MySQL
import model.Price as price


def main(money):
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    sql_del_capital = 'TRUNCATE TABLE my_capital'
    mysql.insert(sql_del_capital)
    sql_install_capital = "INSERT INTO my_capital (capital,money_lock,money_rest)value (%s,%s,%s)" % (money, 0, money)
    mysql.insert(sql_install_capital)
    mysql.commit()

    date_seq = mysql.get_all("SELECT * FROM stock_day")
    for i in date_seq:
        date = i[0]
        # 建模
        price.forecast(date, 0.1)
        # 获取推荐的股票
        ts_code = price.get_forecast()
        print(ts_code, date)
        mysql.close()

# main(100000)
