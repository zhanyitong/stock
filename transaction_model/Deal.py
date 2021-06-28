'''
@Project ：stock 
@File    ：Deal.py
@IDE     ：PyCharm 
@Date    ：2021/6/23 1:19 下午 
@remarks ：获取模拟交易数据库里的剩余金额
'''
import MySQL


class Deal(object):
    cur_capital = 0.00
    cur_money_lock = 0.00
    cur_money_rest = 0.00
    stock_pool = []
    stock_map1 = {}
    stock_map2 = {}
    stock_map3 = {}
    stock_all = []
    ban_list = []

    def __init__(self, state_dt):
        # 建立数据库连接
        mysql = MySQL.MysqlHelper()
        mysql.connect()
        try:
            # 获取模拟资金库里面的资金
            sql_select = 'select * from my_capital a order by seq desc limit 1'
            done_capital = mysql.get_all(sql_select)
            self.cur_capital = 0.00
            self.cur_money_lock = 0.00
            self.cur_money_rest = 0.00
            if len(done_capital) > 0:
                self.cur_capital = float(done_capital[0][0])
                self.cur_money_rest = float(done_capital[0][2])
            # 获取持仓库里面的数据
            sql_select2 = 'select * from my_stock_pool'
            get_pool = mysql.get_all(sql_select2)

            self.stock_pool = []
            self.stock_all = []
            self.stock_map1 = []
            self.stock_map2 = []
            self.stock_map3 = []
            self.ban_list = []
            if len(get_pool) > 0:
                # 获取my_stock数据库里的数据
                self.stock_pool = [x[0] for x in get_pool if x[2] > 0]
                self.stock_all = [x[0] for x in get_pool]
                self.stock_map1 = {x[0]: float(x[1]) for x in get_pool}
                self.stock_map2 = {x[0]: int(x[2]) for x in get_pool}
                self.stock_map3 = {x[0]: int(x[3]) for x in get_pool}
            for i in range(len(get_pool)):
                # 持仓库里的数据跟当天的涨跌进行计算
                sql = "select * from stock_day a where a.stock_code = '%s' and a.state_dt = '%s'" % (
                    get_pool[i][0], state_dt)
                done_temp = mysql.get_all(sql)
                self.cur_money_lock += float(done_temp[0][3]) * float(get_pool[i][2])
        except Exception as excp:
            mysql.rollback()
            print(excp)
        finally:
            mysql.close()
