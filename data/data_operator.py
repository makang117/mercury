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


class DataOperator(object):
    """
    数据操作类
    """
    _db_engine: engine = None
    _db_conn: engine.Connection = None

    def create_db_engine(self) -> engine:
        """
        创建数据引擎
        :return:返回SQLAlchemy数据引擎对象
        """
        try:
            self._db_engine = create_engine(self._conn_str, echo=True, poolclass=NullPool)
            self._db_conn = self._db_engine.connect()
        except Exception as err:
            err.args += ("创建数据操作引擎错误",)
            raise

    @staticmethod
    def get_db():
        db_operator = DataOperator()
        db_operator.create_db_engine()
        return db_operator

    def __init__(self):
        self._conn_str = ""
        sp = SqlParser()
        self._conn_str = sp.parse_conn_engine()  # 获取数据库连接字符串

    @property
    def db_engine(self):
        return self._db_engine

    @property
    def conn_str(self):
        """
        获取数据引擎连接字符串
        :return: 返回数据引擎连接字符串
        """
        return self._conn_str

    @conn_str.setter
    def conn_str(self, value):
        """
        设置数据引擎连接字符串
        :param value: 数据引擎连接字符串
        :return:
        """
        self._conn_str = value

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

    def execute_sql(self, sql):
        """
        执行sql语句
        :param sql:
        :return:
        """
        self._db_conn.execute(sql)

    def query_sql_one(self, sql):
        """
        查询sql语句，返回一个结果
        :param sql:
        :return:返回第一行第一列数据
        """
        result = self.execute_sql(sql)
        row_value = result.scalar()
        return row_value

    def execute_sql(self, sql) -> engine.result.ResultProxy:
        """
        执行SQL语句
        :param sql:
        :return:
        """
        return self._db_conn.execute(sql)

    def dispose(self):
        if self._db_conn:
            self._db_conn.close()

