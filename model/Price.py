# -*- encoding: utf-8 -*-
import MySQL
import threading
import time


def threading_enumerate(number=100):
    while True:
        thread_num = len(threading.enumerate())
        if thread_num <= number:
            break
        time.sleep(0.5)


def calculation(a, b):
    a = float(a)
    b = float(b)
    c = (a - b) / b * 100
    return round(c, 2)


def init(ts_code, accuracy):
    mysql = MySQL.MysqlHelper()
    # 连接MySQL
    mysql.connect()

    stock_daily = mysql.get_all("SELECT * FROM stock_daily WHERE ts_code = '%s'" % ts_code)
    n1 = 0
    for n in stock_daily:
        n1 += 1
        n_date = n[1]
        n_open = n[2]
        n_high = n[3]
        n_low = n[4]
        n_close = n[5]
        n_pct_chg = n[8]

        low_chg = calculation(n_low, n_close)
        high = calculation(n_high, n_close)

        select_price = "SELECT * FROM model_price WHERE high_chg >= '%s' and high_chg <= %s and low_chg >='%s' and " \
                       "low_chg <='%s'" % (high - accuracy, high + accuracy, low_chg - accuracy, low_chg + accuracy)

        get_price = mysql.get_one(select_price)

        rise = 0
        fall = 0
        if n1 < len(stock_daily):
            pct_chg = stock_daily[n1][8]

        if get_price is None:
            # 数据库中没有对应数据存在 写入数据
            if pct_chg > 0:
                rise = 1
                total = pct_chg
            else:
                fall = 1
                total = pct_chg
            model_price = "INSERT INTO model_price(high_chg,low_chg,rise_number,fall_number,total) VALUES ('%s', '%s','%s','%s','%s')" % (
                high, low_chg, rise, fall, total)

            mysql.insert(model_price)
            mysql.commit()
        else:
            id = get_price[0]
            get_rise = get_price[3]
            get_fall = get_price[4]
            get_total = get_price[5]
            if pct_chg > 0:
                get_rise += 1
            else:
                get_fall += 1

            set_total = get_total + pct_chg

            update_model_price = "UPDATE model_price SET rise_number=%s, fall_number=%s , total=%s WHERE id=%s" % (
                get_rise, get_fall, set_total, id)
            # t = threading.Thread(target=threading_install, args=(update_model_price,))
            # t.start()
            mysql.insert(update_model_price)
            mysql.commit()
    mysql.close()


def forecast(date, accuracy):
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    mysql.insert("TRUNCATE TABLE model_price_forecast")
    mysql.commit()
    get_daily = mysql.get_all("SELECT * FROM stock_daily  WHERE trade_date='%s'" % date)
    for i in get_daily:
        threading_enumerate(50)
        t = threading.Thread(target=forecast_threading, args=(i, accuracy,))
        t.start()
    mysql.close()


def forecast_threading(i, accuracy):
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    try:
        n_high = i[3]
        n_low = i[4]
        n_close = i[5]
        low_chg = calculation(n_low, n_close)
        high = calculation(n_high, n_close)
        get_price = mysql.get_one(
            "SELECT * FROM model_price  WHERE high_chg > '%s' and high_chg < '%s'  and low_chg > %s and low_chg > %s" % (
                high - accuracy, high + accuracy, low_chg - accuracy, low_chg + accuracy))
        total = get_price[5]
        fall = get_price[4]
        ries = get_price[3]

        install_price_forecast = "INSERT INTO model_price_forecast(date,ts_code,reis,total) VALUES ('%s', '%s','%s','%s')" % (
            i[0], i[1], ries - fall, total)

        mysql.insert(install_price_forecast)
        mysql.commit()
    except Exception as e:
        pass
    finally:
        mysql.close()


def get_forecast():
    mysql = MySQL.MysqlHelper()
    mysql.connect()
    by_desc = mysql.get_one("SELECT * FROM model_price_forecast order by (reis) desc")
    mysql.close()
    return by_desc[0]


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
                threading_install(sql_insert)
        except Exception as ex:
            print(ex)
    finally:
        mysql.close()
