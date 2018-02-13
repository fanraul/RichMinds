import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf


import R90_tquant.myquant as mt

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

last_trading_datetime = gcf.get_last_trading_daytime()
last_trading_date = last_trading_datetime.date()
last_fetch_date = df2db.get_last_fetch_date(global_module_name, format='date')

def fetch2DB(stockid:str):
    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('Tquant', ['DAILYBAR'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'Tquant'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DAILYBAR'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_dailybar_tquant']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'open':'decimal(8,2)',
                     'close':'decimal(8,2)',
                     'high':'decimal(8,2)',
                     'low':'decimal(8,2)',
                     'vol':'decimal(15,2)',
                     'amount': 'decimal(15,2)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    if last_fetch_date and last_fetch_date >= last_trading_date:
        logprint('No need to fetch dialybar since last_fetch_date %s is later than or equal to last trading date %s' %(last_fetch_date,last_trading_date))
        return

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        mt_stockid = row['Tquant_symbol_ID']
        if last_fetch_date:
            begin_time = last_fetch_date
        elif not row['上市日期']:
            logprint('No stock dailybars can be found for stockid %s' % row['Stock_ID'])
            continue
        elif row['上市日期'].date() > R50_general.general_constants.Global_dailybar_begin_date:
            begin_time = row['上市日期'].date()
        else:
            begin_time = R50_general.general_constants.Global_dailybar_begin_date
        try:
            dfm_stk_info = mt.get_dailybars(mt_stockid,begin_time = begin_time,end_time=last_trading_date)
        except:
            logprint('No stock dailybars can be found for stockid %s' % row['Stock_ID'])
            continue
        # TODO: error handling
        if len(dfm_stk_info) == 0:
            logprint('No stock dailybars can be found for stockid %s' %row['Stock_ID'])
        else:
            # step2: format raw data into prop data type
            # gcf.dfmprint(dfm_stk_info)
            dfm_stk_info.drop(['adj','code'],axis =1,inplace = True)
            gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur)
            # gcf.dfmprint(dfm_stk_info)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_stk_info, table_name,
                                                                dict_misc_pars,
                                                                processing_mode='w_update',float_fix_decimal=2,partial_ind= True)

    if stockid =='':
        df2db.updateDB_last_fetch_date(global_module_name,last_trading_datetime)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)
    logprint('Update last fetch date as %s' % last_trading_datetime)
    df2db.updateDB_last_fetch_date(global_module_name, last_trading_datetime)

if __name__ == '__main__':
    # fetch2DB('300692')
    auto_reprocess()
