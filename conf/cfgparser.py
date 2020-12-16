#!/usr/bin/env python
# coding:utf-8

import json
import os


class ConfigFile(object):
    """
    处理系统配置文件类
    """
    def __init__(self):
        self._file_path = os.path.dirname(os.path.abspath(__file__)) + "/" + self.config_file_name

    @property
    def config_file_name(self):
        """
        获取配置文件名称
        """
        return "configfile.json"

    @property
    def config_file_path(self):
        """
        返回配置文件的路径
        :return: 配置文件路径
        """
        return self._file_path

    @config_file_path.setter
    def config_file_path(self, value):
        """
        设置配置文件路径
        :param value:配置文件路径字符串
        :return:
        """
        if not isinstance(value, str):
            raise ValueError('filepath must be a string!')
        self._file_path = value

    def load_file(self):
        """
        加载配置文件，返回配置文件中的json对象
        :return:配置文件的json对象
        :raises Exception: 打开配置文件错误
        """
        try:
            with open(self._file_path, 'r') as f:
                _load_json = json.load(f)
            return _load_json
        except Exception as err:
            err.args += ('配置文件打开错误！',)
            raise

    def get_dict(self, key) -> dict:
        """
        指定key，返回json字典
        :rtype: object
        :param key: 需要返回json对象的key
        :return: 根据指定的key，返回json对象
        """
        try:
            json_dict = self.load_file()
            dict_value = json_dict[key]
            return dict_value
        except Exception as err:
            err += ('获取节点错误！',)
            raise









