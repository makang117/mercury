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


class TushareObject(object):
    """
    Tushare 对象操作类，获取和操作Tushare接口
    """

    def __init__(self):
        self._key = ""
        self._ts_pro_object = None

    @staticmethod
    def _get_key():
        """
        获取tushare的key
        :return:
        """
        _cfg = ConfigFile()
        tushare_conf = _cfg.get_dict(constants.TUSHARE_KEY)
        return tushare_conf["key"]

    @property
    def key(self):
        """
        获取tushare接口key值
        :return: 接口key值
        """
        if not self._key.strip():
            self._key = self._get_key()
        return self._key

    def get_pro_object(self):
        """
        获取tushare pro接口对象
        :return:
        """
        if not self._ts_pro_object:
            ts.set_token(self.key)
            self._ts_pro_object = ts.pro_api()
        return self._ts_pro_object






