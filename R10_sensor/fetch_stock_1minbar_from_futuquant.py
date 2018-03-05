import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime,timedelta

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf
from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port

'''
1mins数据为高频交易数据
数据获取从2017年2月开始的1分钟线数据
'''

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

end_fetch_datetime = gcf.get_last_trading_daytime()
end_fetch_datetime = datetime(2018,3,2)
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
                     'PCHG': 'decimal(12, 4)',
                    }

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    # for HF trans data, tables should be created by mass_create_HF_dbtables program.
    general_table_name = R50_general.general_constants.dbtables['stock_1minbar_futuquant']
    # df2db.create_stock_HF_tables_by_template(general_table_name, dfm_stocks, table_type='daily_ticks')

    if last_fetch_datetime and last_fetch_datetime.date() >= end_fetch_datetime.date():
        logprint('No need to fetch 1minbar since last_fetch_date %s is later than or equal to end fetch date %s'
                 % (last_fetch_datetime.date(), end_fetch_datetime.date()))
        return
    # buildup connection with futuquant server
    quote_ctx = OpenQuoteContext(api_ip, api_port)

    for index,row in dfm_stocks.iterrows():
        runtime_start = datetime.now()
        api_time = 0
        table_name = general_table_name %(row['Market_ID'] +row['Stock_ID'])
        logprint('Processing stock %s' %row['Stock_ID'])

        mtkstk_id = row['Market_ID'] + '.' + row['Stock_ID']
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

        logprint('fetch %s data from %s to %s:' % (mtkstk_id,begin_time, end_time))
        api_starttime = datetime.now()
        ret,dfm_bars = quote_ctx.get_history_kline(mtkstk_id,
                                                   start=begin_time.strftime('%Y-%m-%d'),
                                                   end=end_time.strftime('%Y-%m-%d'),
                                                   ktype='K_1M',
                                                   autype=None)
        api_time+=(datetime.now() - api_starttime).seconds

        if ret == RET_ERROR:
            assert 0 == 1, 'Error during fetch %s 1minbar , error message: %s' % (mtkstk_id, dfm_bars)

        if len(dfm_bars) == 0:
            logprint('No stock 1minbar can be found for stockid %s' %row['Stock_ID'], ' after futuquant api fetch.')
            continue

        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_bars)
        # dfm_bars.to_excel(gcf.get_tmp_file('futu_%s-1minbar_raw.xlsx' %stockid))
        # futu kline API 返回值会有重复记录,需用drop_plicate函数进行去重处理
        dfm_bars.drop_duplicates(['time_key','low','high','open','close','volume','turnover'],inplace=True)

        dfm_bars['Trans_Datetime'] = dfm_bars.apply(lambda s: datetime.strptime(s['time_key'],'%Y-%m-%d %H:%M:%S'),axis=1)

        dfm_bars.rename(columns = {'change_rate':'PCHG',
                                   'volume':'vol',
                                   'turnover':'amount'},inplace=True)
        dfm_bars.set_index('Trans_Datetime',inplace=True)

        del dfm_bars['time_key']
        del dfm_bars['code']
        del dfm_bars['pe_ratio']
        del dfm_bars['turnover_rate']

        gcf.dfm_col_type_conversion(dfm_bars, columns=dict_cols_cur)

        # dfm_bars.to_excel(gcf.get_tmp_file('futu_%s-1minbar.xlsx' %stockid))

        # gcf.dfmprint(dfm_stk_info)
        db_starttime = datetime.now()
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_bars, table_name,
                                                            dict_misc_pars,
                                                            processing_mode='w_update', float_fix_decimal=4,
                                                            partial_ind=True,is_HF_conn=True)
        db_endtime = datetime.now()
        db_time = (db_endtime-db_starttime).total_seconds()
        runtime_delta = datetime.now() -runtime_start
        logprint("fetch %s 1minbar data from %s to %s takes: total %s minutes; api %s minutes; db %s minutes"
                 %(row['Stock_ID'],begin_time,end_time,runtime_delta.total_seconds()/60,api_time/60,db_time/60))

    quote_ctx.close()

    if stockid =='':
        df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)
    logprint('Update last fetch date as %s' % end_fetch_datetime)
    df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

if __name__ == '__main__':
    # fetch2DB('600000')
    auto_reprocess()
