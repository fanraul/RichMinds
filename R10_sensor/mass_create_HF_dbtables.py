import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime,timedelta

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf

'''
批量基于股票代码生成HF的transaction tables
在HF的数据抽取前,运行本程序保证所有表都已建立.
'''

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


def mass_create_cnstock_dailyticks_tables(dfm_stocks ):
    # for HF trans data, each stock has its own table, so first make sure all tables are created.
    general_table_name = R50_general.general_constants.dbtables['stock_dailyticks_Tquant']
    df2db.create_stock_HF_tables_by_template(general_table_name, dfm_stocks, table_type='daily_ticks')

def mass_create_cnstock_1minbar_tables( dfm_stocks):
    # for HF trans data, each stock has its own table, so first make sure all tables are created.
    general_table_name = R50_general.general_constants.dbtables['stock_1minbar_Tquant']
    df2db.create_stock_HF_tables_by_template(general_table_name, dfm_stocks, table_type='1minbar')

def mass_dbtables_creation():
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist('')
    mass_create_cnstock_dailyticks_tables(dfm_stocks)

if __name__ == '__main__':
    mass_dbtables_creation()
