# encoding:utf-8

"""
@Project : mercury
@FileName: data_operator.py
@Time  : 2019-11-20 10:03
@Author : makang
@EMail: mk117@163.com
@Desc :


"""
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import engine
from data.sqlstringparser import SqlParser
from pandas import DataFrame
import pandas as pd


class DataOperator(object):
    """
    数据操作类
    """
    _db_engine: engine = None

    def create_db_engine(self) -> engine:
        """
        创建数据引擎
        :return:返回SQLAlchemy数据引擎对象
        """
        try:
            self._db_engine = create_engine(self.conn_str, echo=True, poolclass=NullPool)
        except Exception as err:
            err.args += ("创建数据操作引擎错误",)
            raise

    @staticmethod
    def get_db():
        """
        获取数据操作对象静态方法
        :return: 返回DataOperator对象
        """
        db_operator = DataOperator()
        db_operator.create_db_engine()
        return db_operator

    def __init__(self):
        """
        初始化DataOperator对象，获取数据连接字符串
        """
        sp = SqlParser()
        self.conn_str = sp.parse_conn_engine()  # 获取数据库连接字符串

    @property
    def db_engine(self) -> engine:
        """
        获取数据连接的engine实例
        :return: engine实例对象
        """
        return self._db_engine

    @property
    def db_conn(self) -> engine.Connection:
        """
        获取数据连接Connection对象
        :return: Connection实例对象
        """
        return self.db_engine.connect()

    @db_engine.setter
    def db_engine(self, value: engine):
        """
        设置一个新的数据连接engine对象
        :param value: 数据连接engine对象实例
        :return:
        """
        self.db_engine = value

    def insert_from_df(self, df: DataFrame, table_name: str):
        """
        将DataFrame数据插入到数据表中
        :param df: DataFrame数据对象
        :param table_name: 数据表名
        :return:
        """
        if not self.db_engine:
            self.create_db_engine()
        df.to_sql(table_name, self.db_engine, index=False, if_exists='append')

    def query_sql_one(self, sql):
        """
        查询sql语句，返回一个结果
        :param sql:
        :return:返回第一行第一列数据
        """
        result = self.execute_sql(sql)
        row_value = result.scalar()
        return row_value

    def query_to_df(self, sql, var_value):
        """
        查询数据，
        :param sql:
        :param var_value:
        :return:
        """
        try:
            data_df = pd.read_sql_query(sql, self.db_engine, params=var_value)
            return data_df
        except Exception as err:
            err.args += ("error in data_operator.query_to_df() function:", sql, var_value)
            raise

    def execute_sql(self, sql) -> engine.result.ResultProxy:
        """
        执行SQL语句
        :param sql:
        :return:
        """
        return self.db_conn.execute(sql)

    def dispose(self):
        """
        释放数据连接对象
        :return:
        """
        if self.db_conn:
            self.db_conn.close()
        if self.db_engine:
            self.db_engine.dispose()

