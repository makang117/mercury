#!/usr/bin/env python
# coding:utf-8

from conf.cfgparser import ConfigFile
import os


# SqlParse class
class SqlParser(object):
    """
    mysql语句格式化类
    """
    _host = ""
    _port = ""
    _user = ""
    _password = ""
    _db = ""

    def __init__(self):
        self._cfg_path = os.path.abspath('..') + "\\conf\\configfile.json"
        self._get_db_cnf()

    def _get_db_cnf(self):
        """
        构建mysql数据连接数据字典
        :return: mysql数据库连接数据字典
        """
        cfg = ConfigFile()
        cfg.config_file_path = self._cfg_path
        cfg_dict = cfg.get_dict('mysql')
        # 获取配置变量信息
        self._host = cfg_dict['host']
        self._port = cfg_dict['port']
        self._user = cfg_dict['user']
        self._password = cfg_dict['password']
        self._db = cfg_dict['db']

    def parse_conn_engine(self):
        """
        获取sqlalchemy数据库连接字符串
        :return: 返回engine连接字符串
        """
        return f'mysql+pymysql://{self._user}:{self._password}@{self._host}:{self._port}/{self._db}'
