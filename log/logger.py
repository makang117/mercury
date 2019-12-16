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
        ch = logging.StreamHandler()
        ch.setFormatter(format_str)

        # 创建保存日志文件对象
        th = handlers.TimedRotatingFileHandler(filename=self.file_path, when='D', encoding='utf-8')
        th.setFormatter(format_str)

        # 把终端输出和保存日志文件对象加到logger里
        self.logger.addHandler(ch)
        self.logger.addHandler(th)