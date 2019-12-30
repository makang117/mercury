# encoding:utf-8

"""
@Project : mercury
@FileName: import_data.py
@Time  : 2019-11-7 15:38
@Author : makang
@EMail: mk117@163.com
@Desc :
导入数据到数据库

"""
from stock.tushare_object import TushareObject
from data import data_operator as db
import pandas as pd
from datetime import datetime


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
        :return: 返回股票信息列表
        """
        try:
            query_sql = "select * from stock_basic "
            if stock_code.strip():
                query_sql = query_sql + " where basic_code = :basic_code"
                stock_val = {'basic_code': stock_code}
                df_stock = self.db_conn.query_to_df(query_sql, stock_val)
            else:
                df_stock = self.db_conn.query_to_df(query_sql)
            return df_stock
        except Exception as err:
            err.args += ("查询股票基础信息错误：", stock_code)
            raise
        finally:
            self.db_conn.dispose()

    def import_daily(self):
        try:
            query_sql = "SELECT s.basic_code, q.trade_date FROM stock_basic as s LEFT JOIN qfq_daily AS q ON \
                        s.basic_code = q.stock_code GROUP BY s.basic_code"
            stock_df = self.db_conn.query_to_df(query_sql)
            return stock_df
        except Exception as err:
            err.args += ("导入股票日线行情错误",)
            raise
        finally:
            self.db_conn.dispose()

    # def import_stock_daily(self, stock_code, start_date=None, end_date=None):
    #     try:
    #         if (start_date is None):
    #             daily0_df = self.ts

    def last_daily_date(self, stock_code):
        """
        获取股票日线数据的最后日期
        :param stock_code:股票代码
        :return:返回该股票日线数据的最后日期
        """
        if not isinstance(stock_code, str):
            raise ValueError("last_daily_date:股票代码必须为字符串")
        try:
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
                last_date = datetime.strftime(last_date, '%Y-%m-%d')
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






