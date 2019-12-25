# encoding:utf-8

"""
@Project : mercury
@FileName: logger.py
@Time  : 2019-12-11 17:35
@Author : makang
@EMail: mk117@163.com
@Desc :


"""
import logging
from logging import handlers
import os


class Logger(object):
    """
    日志处理对象
    """

    def __init__(self, filename):
        # 定义日志文件和日志格式
        self.file_name = filename
        self.file_path = os.path.dirname(os.path.abspath(__file__)) + "\\" + self.file_name
        fmt = '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        # 创建logger对象
        self.logger = logging.getLogger(filename)
        # 设置日志格式化
        format_str = logging.Formatter(fmt)
        # 设置日志级别
        self.logger.setLevel(logging.DEBUG)
        # 创建屏幕日志显示对象
        self._ch = logging.StreamHandler()
        self._ch.setFormatter(format_str)

        # 创建保存日志文件对象
        self._fh = handlers.TimedRotatingFileHandler(filename=self.file_path, when='D', encoding='utf-8')
        self._fh.setFormatter(format_str)

        # 把终端输出和保存日志文件对象加到logger里
        # self.logger.addHandler(ch)
        # self.logger.addHandler(fh)

    def _add_control_handle(self):
        """
        增加终端日志输出操作句柄对象
        :return:
        """
        if len(self.logger.handlers) > 0:
            # 如果logger对象存在操作句柄，将句柄列表设置为空
            self.logger.handlers = []
        # 增加一个终端日志输出操作句柄对象
        self.logger.addHandler(self._ch)

    def _add_file_handle(self):
        """
        增加日志文件操作句柄对象
        :return:
        """
        if len(self.logger.handlers) > 0:
            # 如果logger对象存在操作句柄，将句柄列表设置为空
            self.logger.handlers = []
        # 增加一个日志文件操作句柄对象
        self.logger.addHandler(self._fh)

    def print_debug(self, msg, *args, **kwargs):
        """
        在终端输出debug级日志
        :param msg:日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_control_handle()
        self.logger.debug(msg, *args, **kwargs)

    def print_info(self, msg, *args, **kwargs):
        """
        在终端输出info级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_control_handle()
        self.logger.info(msg, *args, **kwargs)

    def print_warning(self, msg, *args, **kwargs):
        """
        在终端输出warning级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_control_handle()
        self.logger.warning(msg, *args, **kwargs)

    def print_error(self, msg, *args, **kwargs):
        """
        在终端输出error级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_control_handle()
        self.logger.error(msg, *args, **kwargs)

    def print_critical(self, msg, *args, **kwargs):
        """
        在终端输出critical级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_control_handle()
        self.logger.critical(msg, *args, **kwargs)

    def write_debug(self, msg, *args, **kwargs):
        """
        在日志文件中写入debug级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_file_handle()
        self.logger.debug(msg, *args, **kwargs)

    def write_info(self, msg, *args, **kwargs):
        """
        在日志文件中写入info级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_file_handle()
        self.logger.info(msg, *args, **kwargs)

    def write_warning(self, msg, *args, **kwargs):
        """
        在日志文件中写入warning级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_file_handle()
        self.logger.warning(msg, *args, **kwargs)

    def write_error(self, msg, *args, **kwargs):
        """
        在日志文件中写入error级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_file_handle()
        self.logger.error(msg, *args, **kwargs)

    def write_critical(self, msg, *args, **kwargs):
        """
        在日志文件中写入critical级日志
        :param msg: 日志信息
        :param args:
        :param kwargs:
        :return:
        """
        self._add_file_handle()
        self.logger.critical(msg, *args, **kwargs)






