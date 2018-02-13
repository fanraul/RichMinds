"""
得到一个指数下面所有的股票信息
"""
# allow this program run as a job by itself.
import sys
sys.path.append(r"C:\00_RichMinds\Github\RichMinds")

from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port
import pandas as pd
from pandas import Series, DataFrame
import numpy as np

import re

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf


global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

# this flag control the scope of the IDX to be processed
# 1: only processing the entry which manualflg_no_stocks_under_idx IS NULL
#   (causion, must be NULL or 'X', user should not maintain other value)
# 2: only processing the entry with manualflg_no_stocks_under_idx = 'X'
# any other values: don't consider manualflg_no_stocks_under_idx flg
mode_manualflg_no_stocks_under_idx = 1

def fetch2DB():
    dfm_db_chars = df2db.get_chars('FUTUQUANT', ['IDXSTOCK'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'FUTUQUANT'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'Y'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'IDXSTOCK'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_index_stocks_futuquant']
    df2db.create_table_by_template(table_name, table_type='stock_date_multi_value')
    dict_cols_cur = {'Sub_Stock_ID': 'nvarchar(50)',
                     'lot_size': 'int',
                     'owner_market': 'nvarchar(10)',
                     'stock_child_type': 'nvarchar(10)',
                     'stock_type':'nvarchar(10)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    dfm_idxs = get_idx_all(mode_manualflg_no_stocks_under_idx)
    ls_dfm_idxstocks_all =[]
    ls_mktidxs_dead_to_alive = []
    for index, row in dfm_idxs.iterrows():
        code = row['Market_ID']+'.'+row['Stock_ID']
        logprint('Processing index %s' %code)
        ret, dfm_idx_stocks = gcf.get_plate_stocks_futuquant(api_ip, api_port, code)
        if ret == RET_ERROR:
            logprint('Failed to get stocks under idx %s. Err message: %s' %(code,dfm_idx_stocks),add_log_files='I')
        else:
            dfm_idx_stocks['Sub_Stock_ID'] = dfm_idx_stocks.apply(lambda s:gcf.get_mkt_stk_futuquant(s['code'])[1] ,axis =1)

            del dfm_idx_stocks['stock_name']
            del dfm_idx_stocks['code']
            gcf.dfm_col_type_conversion(dfm_idx_stocks, columns=dict_cols_cur)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_idx_stocks,
                                                            table_name,
                                                            dict_misc_pars,
                                                            process_mode='w_check')
            ls_dfm_idxstocks_all.append(dfm_idx_stocks)
            ls_mktidxs_dead_to_alive.append(row['Market_ID']+'.'+row['Stock_ID'])
    # pd.concat(ls_dfm_idxstocks_all).to_excel('idx_stocks_all.xls')
    if mode_manualflg_no_stocks_under_idx == 2 and len(ls_mktidxs_dead_to_alive) > 0:
        gcf.send_email(R50_general.general_constants.Global_email_receiver, 'Some IDXs have stocks assigned, please check!',
               """please update column manualflg_no_stocks_under_idx as 'X' for below indexes:
               """ + '\n'.join(ls_mktidxs_dead_to_alive),
               [])

def get_idx_all(mode_manualflg_no_stocks_under_idx):
    table_name = R50_general.general_constants.dbtables['stocklist_hkus_futuquant']
    ls_cond = [{'db_col': 'stock_type', 'db_oper': '=', 'db_val': "'IDX'"},]
    if mode_manualflg_no_stocks_under_idx == 1:
        ls_cond.append({'db_col': 'manualflg_no_stocks_under_idx', 'db_oper': 'IS', 'db_val': " NULL "})
    elif mode_manualflg_no_stocks_under_idx == 2:
        ls_cond.append({'db_col': 'manualflg_no_stocks_under_idx', 'db_oper': '=', 'db_val': "'X'"})
    dfm_cond = DataFrame(ls_cond)
    return df2db.get_data_from_DB(table_name, dfm_cond)

if __name__ == "__main__":
    # print(get_index_stocks(api_ip, api_port, 'HK.800000'))
    mode_manualflg_no_stocks_under_idx = 2
    fetch2DB()

