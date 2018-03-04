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
1mins数据为高频交易数据
数据获取从2017年2月开始的1分钟线数据
'''

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

end_fetch_datetime = gcf.get_last_trading_daytime()
# end_fetch_datetime = datetime(2017,3,6)
# last_trading_date = last_trading_datetime.date()
last_fetch_datetime = df2db.get_last_fetch_date(global_module_name)

def fetch2DB(stockid:str):
    # 1.1 init step
    dict_misc_pars = {}
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name

    dict_cols_cur = {'high':'decimal(8,2)',
                     'close':'decimal(8,2)',
                     'low': 'decimal(8,2)',
                     'open': 'decimal(8,2)',
                     'vol':'decimal(15,2)',
                     'amount': 'decimal(15,2)',
                    }

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    # for HF trans data, tables should be created by mass_create_HF_dbtables program.
    general_table_name = R50_general.general_constants.dbtables['stock_1minbar_Tquant']
    # df2db.create_stock_HF_tables_by_template(general_table_name, dfm_stocks, table_type='daily_ticks')

    if last_fetch_datetime and last_fetch_datetime.date() >= end_fetch_datetime.date():
        logprint('No need to fetch 1minbar since last_fetch_date %s is later than or equal to end fetch date %s'
                 % (last_fetch_datetime.date(), end_fetch_datetime.date()))
        return

    for index,row in dfm_stocks.iterrows():
        runtime_start = datetime.now()
        api_time = 0
        table_name = general_table_name %(row['Market_ID'] +row['Stock_ID'])
        logprint('Processing stock %s' %row['Stock_ID'])

        mt_stockid = row['Tquant_symbol_ID']
        if last_fetch_datetime:
            begin_time = last_fetch_datetime
        elif not row['上市日期']:
            logprint('No need to fetch stock 1minbar for stockid %s' % row['Stock_ID'], ' due to stock has no 上市日期')
            continue
        elif row['上市日期'].date() > R50_general.general_constants.Global_1minbar_begin_datetime.date():
            begin_time = row['上市日期']
        else:
            begin_time = R50_general.general_constants.Global_1minbar_begin_datetime
        # try:
        end_time = end_fetch_datetime
        if begin_time > end_time:
            logprint('No need to fetch stock 1minbar for stockid %s' % row['Stock_ID'], ' due to stock not yet 上市')
            continue


        # delta_days = (end_time-begin_time).days
        # ls_ticks_info =[]
        # begin_time_tmp = begin_time
        # for i in range(delta_days+1):
        #     end_time_tmp = begin_time_tmp + timedelta(days = 1)
        #     api_starttime = datetime.now()
        #     dfm_tick_day = mt.get_ticks(mt_stockid,begin_time = begin_time_tmp,end_time=end_time_tmp)
        #     api_endtime = datetime.now()
        #     api_time +=(api_endtime-api_starttime).total_seconds()
        #     begin_time_tmp = end_time_tmp
        #     if len(dfm_tick_day) > 0:
        #         ls_ticks_info.append(dfm_tick_day)

        ls_1minbar_info,api_time = call_api_by_interval(mt_stockid, begin_time, end_time, 30)

        if len(ls_1minbar_info) > 0:
            dfm_bars = pd.concat(ls_1minbar_info)
        else:
            logprint('No stock 1minbar can be found for stockid %s' %row['Stock_ID'], ' after tquant api fetch.')
            continue

        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_bars)
        # dfm_bars['Trans_Datetime'] = dfm_bars.index
        del dfm_bars['code']
        del dfm_bars['adj']

        # dfm_bars.to_excel(gcf.get_tmp_file('%s-ticks.xlsx' %stockid))

        gcf.dfm_col_type_conversion(dfm_bars, columns=dict_cols_cur)
        # gcf.dfmprint(dfm_stk_info)
        db_starttime = datetime.now()
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_bars, table_name,
                                                            dict_misc_pars,
                                                            processing_mode='w_update', float_fix_decimal=2,
                                                            partial_ind=True,is_HF_conn=True)
        db_endtime = datetime.now()
        db_time = (db_endtime-db_starttime).total_seconds()
        runtime_delta = datetime.now() -runtime_start
        logprint("fetch %s ticks data from %s to %s takes: total %s minutes; api %s minutes; db %s minutes"
                 %(row['Stock_ID'],begin_time,end_time,runtime_delta.total_seconds()/60,api_time/60,db_time/60))
    if stockid =='':
        df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)
    logprint('Update last fetch date as %s' % end_fetch_datetime)
    df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

def call_api_by_interval(mt_stockid,begin_time,end_time,days_interval):
    """
    由于每个api返回值最多是30000行,所以当取数的时间间隔过大时,会有数据遗漏,所以可以设置一个days_interval,比如20天,则将一个大的取数时间
    分为多个20天的小间隔进行取数,从而保证不会出现一次返回数据量过大导致遗漏的问题.
    :param begin_time:
    :param end_time:
    :param days_interval:
    :return:
    """
    delta_days = (end_time - begin_time).days
    interval_times = delta_days // days_interval

    ls_data_info = []
    begin_time_tmp = begin_time
    api_time = 0

    for i in range(interval_times +1):
        if i == interval_times:
            end_time_tmp = end_time
        else:
            end_time_tmp = begin_time_tmp + timedelta(days=days_interval)
        print('fetch data from %s to %s:' %(begin_time_tmp,end_time_tmp))
        api_starttime = datetime.now()
        dfm_1minbar = mt.get_bars(mt_stockid, 1 * 60, begin_time_tmp, end_time_tmp)
        api_endtime = datetime.now()
        api_time += (api_endtime - api_starttime).total_seconds()
        begin_time_tmp = end_time_tmp
        if len(dfm_1minbar) > 0:
            ls_data_info.append(dfm_1minbar)

    return ls_data_info,api_time


if __name__ == '__main__':
    # fetch2DB('600000')
    auto_reprocess()
