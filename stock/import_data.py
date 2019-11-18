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
from sqlalchemy import create_engine
from data.sqlstringparser import SqlParser


# Basic_Data class
class BasicData(object):
    """
    导入股票基础数据类
    """
    _ts = None
    _ts_pro = None
    _conn_str = ""
    _conn = None

    def __init__(self):
        if not self._ts_pro:
            self._ts = TushareObject()
            self._ts_pro = self._ts.get_pro_object()

    def _init_conn(self):
        """
        初始化数据库连接字符串及连接对象
        :return:
        """
        sp = SqlParser()
        self._conn_str = sp.parse_conn_engine()
        self._conn = create_engine(self._conn_str)



    def stock_list(self):
        _stock_list = self._ts_pro.query('stock_basic', exchange='', fields='ts_code, symbol, name, area, industry, \
                                         fullname, enname, market, exchange, curr_type, list_status, delist_date, is_hs')
        return _stock_list
