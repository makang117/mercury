# encoding:utf-8

"""
@Project : mercury
@FileName: import_data.py
@Time  : 2019-11-7 15:38
@Author : makang
@EMail: mk117@163.com
@Desc :
导入股票基础数据。
BasicData:股票的基础数据操作类
    query_stock:
    query_last_daily:
    import_daily:
    import_stock_daily:
    last_daily_date:
    import_stock:
"""
from stock.tushare_object import TushareObject
from data import data_operator as db
# import pandas as pd
import datetime
import time


# Basic_Data class
class BasicData(object):

    def __init__(self):
        try:
            self.ts = TushareObject()
        except Exception as err:
            err.args += ("创建TuShare对象错误", err)
            raise
        try:
            self.db_conn = db.DataOperator.get_db()  # 初始化数据操作类
            """
            :param db_conn:数据操作对象
            """
        except Exception as err:
            err.args += ("创建数据操作对象错误", err)
            raise

    def query_stock(self, stock_code=""):
        """
        根据股票代码查询股票基本信息，
        若股票代码为空，查询所有的股票信息
        :param stock_code: 股票代码，若股票代码为空，查询所有的股票信息
        :return: 返回股票信息列表 -> DataFrame
        """
        try:
            query_sql = "select * from stock_basic "
            # 如果股票代码不为空，查询该股票的基础信息
            if stock_code.strip():
                query_sql = query_sql + " where basic_code = :basic_code"
                stock_val = {'basic_code': stock_code}
                df_stock = self.db_conn.query_to_df(query_sql, stock_val)
            else:
                # 如果股票代码为空，查询所有的股票基础信息
                df_stock = self.db_conn.query_to_df(query_sql)
            return df_stock
        except Exception as err:
            err.args += ("查询股票基础信息错误：", stock_code)
            raise
        finally:
            self.db_conn.dispose()

    def import_daily(self):
        """
        导入所有股票的行情数据
        :return:导入股票的数量。
                返回系统中所有股票的数量
        """
        try:
            # 获取所有股票的数量
            stock_df = self.query_stock()
            # 循环股票列表
            for row in stock_df.itertuples():
                basic_code = getattr(row, 'basic_code')
                # 获取最新的交易日期
                last_daily = self.get_last_daily(basic_code)
                if last_daily is None:
                    # self.import_stock_daily(basic_code)
                    start_date = datetime(2017, 1, 1)
                else:
                    # 开始日期推后一天
                    start_date = last_daily + datetime.timedelta(days=1)
                    # 结束日期设置为当前日期
                end_date = datetime.date.today()
                # 导入股票日线行情，交易日期大于当前系统最新的交易日期
                self.import_stock_daily(basic_code, start_date, end_date)
                # time.sleep(1)
            return stock_df.shape[0]
        except Exception as err:
            err.args += ("导入股票日线行情错误", err)
            raise
        finally:
            self.db_conn.dispose()

    def import_stock_daily(self, stock_code, start_date=None, end_date=None):
        """
        导入股票的日线数据
        :param stock_code: 股票代码
        :param start_date: 行情开始时间
        :param end_date: 行情结束时间
        :return: 股票代码， 导入股票日线数据的数量
                stock_code:股票代码
                daily_num:股票日线行情的交易天数
        """
        try:
            # 从tushare接口获取股票日线行情
            daily_df = self.ts.query_stock_daily(stock_code, start_date, end_date)
            daily_num = 0
            # 如果日线行情不为空，导入日线行情
            if not(daily_df is None):
                self.db_conn.insert_from_df(daily_df, "qfq_daily")
                daily_num = daily_df.shape[0]
            # 返回股票代码、导入股票日线行情的交易天数
            return stock_code, daily_num
        except Exception as err:
            err.args += ("导入股票日线行情数据错误", stock_code, err)
            raise
        finally:
            self.db_conn.dispose()

    def get_last_daily(self, stock_code):
        """
        获取股票日线数据的最后日期
        :param stock_code:股票代码
        :return:返回该股票日线数据的最后日期
        """
        if not isinstance(stock_code, str):
            raise ValueError("last_daily_date:股票代码必须为字符串")
        try:
            # 获取股票日线行情的最新日期
            query_sql = "SELECT max(trade_date) FROM qfq_daily WHERE stock_code = :stock_code"
            stock_val = {'stock_code': stock_code}
            last_date = self.db_conn.query_sql_one(query_sql, stock_val)
            return last_date
        except Exception as err:
            err.args += ("获取股票日线数据最后一天错误", stock_code)
            raise
        finally:
            self.db_conn.dispose()

    def import_stock(self):
        """
        导入股票基础信息
        :return:导入股票的数量
        """
        try:
            # 从tushare接口中获取股票列表
            stock_list = self.ts.query_stock_list()

            # 获取数据库中股票列表最新的上市股票日期
            max_date_sql = 'select max(stock_basic.list_date) from stock_basic'
            last_date = self.db_conn.query_sql_one(max_date_sql)

            if not(last_date is None):  # 如果时间不为空，对比数据库和股票列表数据
                last_date = datetime.datetime.strftime(last_date, '%Y-%m-%d')
                mask = (stock_list['list_date'] > last_date)
                stock_list = stock_list.loc[mask]  # 过滤股票数据，选择最新的股票信息
            real_num = stock_list.shape[0]
            if real_num > 0:
                self.db_conn.insert_from_df(stock_list, 'stock_basic')
            return real_num  # 导入成功,返回插入股票信息的数量
        except Exception as err:
            err.args += ("导入股票基础数据错误", err)
            raise
        finally:
            self.db_conn.dispose()






