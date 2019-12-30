# encoding:utf-8

"""
@Project : mercury
@FileName: tushare_object.py
@Time  : 2019-11-7 22:24
@Author : makang
@EMail: mk117@163.com
@Desc :
tushare 对象操作类；主要包括初始化tushare参数、获取tushuare接口等

"""
import tushare as ts
from conf.cfgparser import ConfigFile
from . import constants
import pandas as pd


class TushareObject(object):
    """
    Tushare 对象操作类，获取和操作Tushare接口
    """
    def __init__(self):
        cfg = ConfigFile()
        tushare_conf = cfg.get_dict(constants.TUSHARE_KEY)
        self.key = tushare_conf["key"]
        ts.set_token(self.key)
        self.ts_pro = ts.pro_api()

    def query_stock_list(self):
        try:
            # 从接口中获取最新的股票基础数据
            stock_list = self.ts_pro.query('stock_basic', exchange='', fields='ts_code, symbol, name, area, industry,\
                           fullname, enname, market, exchange, curr_type, list_status, list_date, delist_date, is_hs')

            # 修改接口中的列名称，保持和数据库的列名称相同
            stock_list.rename(columns={'ts_code': 'basic_code', 'name': 'stock_name', 'fullname': 'full_name',
                                       'enname': 'en_name'}, inplace=True)

            # 转换股票列表DataFrame的上市时间、退市时间列数据类型
            stock_list['list_date'] = pd.to_datetime(stock_list['list_date'])
            stock_list['delist_date'] = pd.to_datetime(stock_list['delist_date'])
            return stock_list
        except Exception as err:
            err.args += ("tushare接口获取股票列表错误", err)
            raise
