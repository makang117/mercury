
# encoding:utf-8
import conf.cfgparser as conf
from stock.tushare_object import TushareObject
from data.sqlstringparser import SqlParser
from stock.import_data import BasicData
from log.logger import Logger
import time
import datetime


def test_conf():
    config_file = conf.ConfigFile()
    dict_key = config_file.get_dict("mysql")
    print(type(dict_key))
    print(config_file.config_file_path)


def test_sql_string():
    _sql = SqlParser()
    engine_str = _sql.parse_conn_engine()
    print(engine_str)


def test_ts():
    ts_object = TushareObject()
    t = datetime.date(2003, 1, 6)
    t1 = datetime.date(2003, 1, 7)
    df = ts_object.query_stock_daily('600018.SH', start_date=t, end_date=t1)
    print(df.dtypes)
    print(df)


def test_log():
    log = Logger("test.log")
    log.print_debug("debug test")
    log.write_info("info test")
    log.print_debug("debug test1")

    for handler in log.logger.handlers:
        print(handler)


def test_import_daily():
    log = Logger("import_data.log")
    try:
        stock_data = BasicData()
        stock_df = stock_data.query_stock()
        for row in stock_df.itertuples():
            basic_code = getattr(row, 'basic_code')
            last_daily = stock_data.get_last_daily(basic_code)
            if last_daily is None:
                stock_code, daily_num = stock_data.import_stock_daily(basic_code)
            else:
                start_date = last_daily + datetime.timedelta(days=1)
                # 结束日期设置为当前日期
                end_date = datetime.date.today()
                # 导入股票日线行情，交易日期大于当前系统最新的交易日期
                stock_code, daily_num = stock_data.import_stock_daily(basic_code, start_date, end_date)
            log.print_info("导入股票：" + str(stock_code) + "  " + str(daily_num) + "条行情数据")
            time.sleep(1)
        log.print_info("系统导入" + str(stock_df.shape[0]) + "条股票行情数据！")
        log.print_info("系统导入" + str(stock_df.shape[0]) + "条股票行情数据！")
    except Exception as err:
        log.print_error("导入股票日线行情错误：" + str(err))
        log.write_error("导入股票日线行情错误：" + str(err))


def test_import_stock():
    log = Logger("import_data.log")
    try:
        stock_data = BasicData()
        stock_num = stock_data.import_stock()
        log.print_info("导入" + str(stock_num) + "条股票数据")
        log.write_info("导入" + str(stock_num) + "条股票数据")
    except Exception as err:
        log.print_error("导入股票基础信息错误：" + str(err))
        log.write_error("导入股票基础信息错误：" + str(err))


if __name__ == "__main__":
    # test_conf()
    # test_ts()
    # test_import_stock()
    test_import_daily()
    # test_log()
    # test_sql_string()
    # test_vector()





