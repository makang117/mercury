
# encoding:utf-8

from test.vector import Vector
import conf.cfgparser as conf
from stock import tushare_object as tsobject
from data.sqlstringparser import SqlParser
from stock.import_data import BasicData
import tushare


def test_vector():
    vec = Vector([5, 2])
    print(vec)
    vec2 = Vector([3, 1])
    print("{} + {} = {}".format(vec, vec2, vec + vec2))

    zero2 = Vector.zero(2)
    print(zero2)
    print("{} + {} = {}".format(vec, zero2, vec + zero2))
    print("norm{}  = {}".format(vec, vec.norm))


def test_conf():
    config_file = conf.ConfigFile()
    print(config_file.config_file_path)


def test_sql_string():
    _sql = SqlParser()
    engine_str = _sql.parse_conn_engine()
    print(engine_str)


def test_ts():
    to = tsobject.TushareObject()
    to_pro = to.get_pro_object()
    data = to_pro.query('stock_basic', exchange='', fields='is_hs')
    print(tushare.__version__)
    print(data)
    print(type(data))


def test_import_data():
    stock_data = BasicData()
    s_list = stock_data.stock_list()
    print(s_list)
    print(s_list.shape[1])


if __name__ == "__main__":
    # test_conf()
    # test_ts()
    test_import_data()
    # test_sql_string()
    # test_vector()





