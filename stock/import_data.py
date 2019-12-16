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
import datetime


# Basic_Data class
class BasicData(object):
    """
    导入股票基础数据类
    """
    _ts = None
    _ts_pro = None
    _db_conn: db.DataOperator = None

    def __init__(self):
        if not self._ts_pro:
            try:
                self._ts = TushareObject()
                self._ts_pro = self._ts.get_pro_object()
            except Exception as err:
                err.args += ("创建TuShare对象错误",)
                raise
        if not self._db_conn:
            try:
                self._db_conn = db.DataOperator.get_db()  # 初始化数据操作类
            except Exception as err:
                err.args += ("创建数据操作对象错误",)
                raise

    @property
    def db_conn(self) -> db.DataOperator:
        """
        获取数据操作对象
        :return:返回数据操作对象
        """
        return self._db_conn

    @db_conn.setter
    def db_conn(self, value: db.DataOperator):
        """
        设置数据操作对象实例
        :param value: 数据对象实例
        :return:
        """
        self.db_conn = value

    def import_stock(self):
        """
        导入股票基础信息
        :return:导入股票的数量
        """
        try:
            _real_num = 0
            stock_list = self._ts_pro.query('stock_basic', exchange='', fields='ts_code, symbol, name, area, industry,\
                           fullname, enname, market, exchange, curr_type, list_status, list_date, delist_date, is_hs')
            # 从接口中获取最新的股票基础数据

            stock_list.rename(columns={'ts_code': 'basic_code', 'name': 'stock_name', 'fullname': 'full_name',
                                        'enname': 'en_name'}, inplace=True)
            # 修改接口中的列名称，保持和数据库的列名称相同

            stock_list['list_date'] = pd.to_datetime(stock_list['list_date'])
            stock_list['delist_date'] = pd.to_datetime(stock_list['delist_date'])
            # 转换股票列表DataFrame的上市时间、退市时间列数据类型

            last_date = self.db_conn.query_sql_one('select max(stock_basic.list_date) from stock_basic')
            # 获取数据库中股票列表最新的上市股票日期
            if not(last_date is None):  # 如果时间不为空，对比数据库和股票列表数据
                last_date = datetime.datetime.strftime(last_date, '%Y-%m-%d')
                mask = (stock_list['list_date'] > last_date)
                stock_list = stock_list.loc[mask]  # 过滤股票数据，选择最新的股票信息
            _real_num = stock_list.shape[0]
            insert_num = 0
            if _real_num > 0:
                self.db_conn.insert_from_df(stock_list, 'stock_basic')
            insert_num = _real_num  # 如果导入成功，获取导入数据的数量
        except Exception as err:
            err.args += ("导入股票基础数据错误",)
            raise
        finally:
            self.db_conn.dispose()
            return insert_num  # 返回插入股票信息的数量





