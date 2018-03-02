import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime,timedelta

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf


import R90_tquant.myquant as mt

'''
Ticks数据为高频交易数据
此数据源当天不会更新当天的数据,本交易日只能获得昨日数据,故不建议每日更新,而是每周日更新.
'''

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


# manually set end time for cut-over.
end_fetch_datetime = gcf.get_last_trading_daytime()
end_fetch_datetime = datetime(2018,1,31,23,0)

# last_trading_date = last_trading_datetime.date()
last_fetch_datetime = df2db.get_last_fetch_date(global_module_name)

def fetch2DB(stockid:str):
    # init step
    table_name = R50_general.general_constants.dbtables['stock_dailyticks_Tquant']
    dict_misc_pars = {}
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name

    dict_cols_cur = {'tick_Datetime':'datetime',
                     'amount': 'decimal(15,2)',
                     'close':'decimal(8,2)',
                     'opi':'decimal(8,2)',
                     'vol':'decimal(15,2)',
                     '买一价': 'decimal(8,2)',
                     '买一量': 'decimal(15,2)',
                     '卖一价': 'decimal(8,2)',
                     '卖一量': 'decimal(15,2)',
                    }

    # for HF trans data, DB is created before, no db structure adjustment in program

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    if last_fetch_datetime and last_fetch_datetime.date() >= end_fetch_datetime.date():
        logprint('No need to fetch dialyticks since last_fetch_date %s is later than or equal to end fetch date %s'
                 % (last_fetch_datetime.date(), end_fetch_datetime.date()))
        return

    for index,row in dfm_stocks.iterrows():
        runtime_start = datetime.now()
        logprint('Processing stock %s' %row['Stock_ID'])
        mt_stockid = row['Tquant_symbol_ID']
        if last_fetch_datetime:
            begin_time = last_fetch_datetime
        elif not row['上市日期']:
            logprint('No need to fetch stock dailyticks for stockid %s' % row['Stock_ID'], ' due to stock has no 上市日期')
            continue
        elif row['上市日期'].date() > R50_general.general_constants.Global_dailyticks_begin_datetime.date():
            begin_time = row['上市日期']
        else:
            begin_time = R50_general.general_constants.Global_dailyticks_begin_datetime
        # try:
        end_time = end_fetch_datetime
        if begin_time > end_time:
            logprint('No need to fetch stock dailyticks for stockid %s' % row['Stock_ID'], ' due to stock not yet 上市')
            continue

        delta_days = (end_time-begin_time).days
        ls_ticks_info =[]
        begin_time_tmp = begin_time
        for i in range(delta_days+1):
            end_time_tmp = begin_time_tmp + timedelta(days = 1)
            dfm_tick_day = mt.get_ticks(mt_stockid,begin_time = begin_time_tmp,end_time=end_time_tmp)
            begin_time_tmp = end_time_tmp
            if len(dfm_tick_day) > 0:
                ls_ticks_info.append(dfm_tick_day)
        if len(ls_ticks_info) > 0:
            dfm_ticks = pd.concat(ls_ticks_info)
        else:
            logprint('No stock dailyticks can be found for stockid %s' %row['Stock_ID'], ' after tquant api fetch.')
            continue

        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_ticks)
        dfm_ticks['tick_Datetime'] = dfm_ticks.index
        dfm_ticks['Trans_Datetime'] = dfm_ticks.apply(lambda s: s['tick_Datetime'].date(), axis=1)
        dfm_ticks.set_index('Trans_Datetime', inplace=True)
        del dfm_ticks['code']
        # dfm_ticks.to_excel(gcf.get_tmp_file('%s-ticks.xlsx' %stockid))

        gcf.dfm_col_type_conversion(dfm_ticks, columns=dict_cols_cur)
        # gcf.dfmprint(dfm_stk_info)
        df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                           row['Stock_ID'],
                                                           dfm_ticks,
                                                           table_name,
                                                           dict_misc_pars,
                                                           float_fix_decimal =2,
                                                           partial_ind=True,
                                                           is_HF_conn=True)
        runtime_delta = datetime.now() -runtime_start
        logprint("fetch %s ticks data from %s to %s takes: %s minutes" %(row['Stock_ID'],begin_time,end_time,runtime_delta.total_seconds()/60))
    if stockid =='':
        df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)
    logprint('Update last fetch date as %s' % end_fetch_datetime)
    df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

if __name__ == '__main__':
    # fetch2DB('300692')
    auto_reprocess()
