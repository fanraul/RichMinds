import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf


import R90_tquant.getdata as gt

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

"""
this report use tquant module's get_financial func to get balance report and store them into sql server
get_financial func return 3 dataframe, the first one is balance sheet, the second is profit, and the
third is cash flow.

once program get this 3 reports, it call general funcs in dfm_to_table_common to do 3 things:
1) check table exist or not, if not create balance report table
2) check feature exist or not, if not, add feature to char table and add feature to balance table as new columns
3) insert or update the balance table
"""

#TODO: 1.add error handling and log output
#TODO: 2.如何获得当前的程序名称作为创建人或修改者


def fetch2DB(stockid:str = ''):
    # init step
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    # step2:loop at stock list and fetch and save to DB
    for item in dfm_stocks['Stock_ID']:         # get column Stock_ID from dataframe
        dfm_db_chars = df2db.get_chars('Tquant',['FIN10',"FIN20",'FIN30'])
        fetch2DB_individual(item,dfm_db_chars)               # item is str type

def fetch2DB_individual(item : str, dfm_db_chars):
    """
    get finreports columns and update ZCFG_character and alter transaction table
    :param item:
    :param dfm_db_chars:
    :return:
    """
    # step2.2: get current character list in each loop so that new chars are included.
    logprint('Processing:',item)
    try:
        ls_finreports = gt.get_financial(item)
    except:
        logprint("Stock %s can't get financial information" %item)
        return

    ls_usage =['FIN10',"FIN20",'FIN30']

    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'Tquant'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] ='N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name

    for i in range(len(ls_finreports)):
        #get db table name for fin report
        trans_table_name = R50_general.general_constants.dbtables['finpreports_Tquant'][i]
        #check table exist or not, if not create it by template string
        df2db.create_table_by_template(trans_table_name,table_type='stock_date')
        dfm_fin=ls_finreports[i]
        dict_misc_pars['char_usage'] = ls_usage[i]
        dict_cols_cur = {x: "decimal(18,6)" for x in dfm_fin.columns}
        df2db.add_new_chars_and_cols(dict_cols_cur,
                                     list(dfm_db_chars['Char_ID'][dfm_db_chars.Char_Usage == ls_usage[i]]),
                                     trans_table_name,dict_misc_pars)


        # step4: insert transaction data into transaction table
        market_id = 'SH' if item.startswith('6') else 'SZ'
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(market_id, item, dfm_fin, trans_table_name, dict_misc_pars, processing_mode ='w_update')


def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    # fetch2DB('300692')
    auto_reprocess()
