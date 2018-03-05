import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf

from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port

'''
此数据源只有等服务器端的数据都更新后,才能使用,故不建议每日更新,而是每周日更新.
'''

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

# last_trading_date = last_trading_datetime.date()

end_fetch_datetime = gcf.get_last_trading_daytime()
end_fetch_datetime = datetime(2018,3,2,23)

last_fetch_datetime = df2db.get_last_fetch_date(global_module_name)

def fetch2DB(stockid:str):
    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('futuquant', ['DAILYBAR'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'futuquant'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DAILYBAR'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_dailybar_futuquant']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'open':'decimal(8,2)',
                     'close':'decimal(8,2)',
                     'high':'decimal(8,2)',
                     'low':'decimal(8,2)',
                     'vol':'decimal(15,2)',
                     'amount': 'decimal(15,2)',
                     'PCHG':'decimal(10, 4)',
                     'pe_ratio':'decimal(15, 4)',
                     'turnover_rate':'decimal(10, 4)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    if last_fetch_datetime and last_fetch_datetime >= end_fetch_datetime.date():
        logprint('No need to fetch dialybar since last_fetch_date %s is later than or equal to end fetch date %s' %(last_fetch_datetime.date(),
                                                                                                                    end_fetch_datetime.date()))
        return

    # buildup connection with futuquant server
    quote_ctx = OpenQuoteContext(api_ip, api_port)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        mtkstk_id = row['Market_ID'] + '.' + row['Stock_ID']

        (is_fetch_required, begin_time, end_time) = gcf.determine_time_period_to_fetch_per_stock(mtkstk_id,
                                                                                             last_fetch_datetime,
                                                                                             row['上市日期'],
                                                                                             end_fetch_datetime,
                                                                                             data_type = 'dailybar')

        if not is_fetch_required:
            continue

        logprint('fetch %s data from %s to %s:' % (mtkstk_id, begin_time, end_time))
        ret, dfm_bars = quote_ctx.get_history_kline(mtkstk_id,
                                                    start=begin_time.strftime('%Y-%m-%d'),
                                                    end=end_time.strftime('%Y-%m-%d'),
                                                    ktype='K_DAY',
                                                    autype=None,
                                                    )

        if ret == RET_ERROR:
            logprint('Error during fetch %s dailybar , error message: %s' % (mtkstk_id, dfm_bars), add_log_files='I')
            continue

        if len(dfm_bars) == 0:
            logprint('No stock dailybars can be found for stockid %s' %row['Stock_ID'])
            continue


        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)
        # futu kline API 返回值会有重复记录,需用drop_plicate函数进行去重处理
        dfm_bars.drop_duplicates(['time_key','low','high','open','close','volume','turnover'],inplace=True)

        dfm_bars['Trans_Datetime'] = dfm_bars.apply(lambda s: datetime.strptime(s['time_key'],'%Y-%m-%d %H:%M:%S'),axis=1)

        dfm_bars.rename(columns = {'change_rate':'PCHG',
                                   'volume':'vol',
                                   'turnover':'amount'},inplace=True)
        dfm_bars.set_index('Trans_Datetime',inplace=True)

        dfm_bars.drop(['time_key','code'],axis =1,inplace = True)

        gcf.dfm_col_type_conversion(dfm_bars, columns=dict_cols_cur)
        # gcf.dfmprint(dfm_stk_info)
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_bars, table_name,
                                                            dict_misc_pars,
                                                            processing_mode='w_update',float_fix_decimal=4,
                                                            partial_ind= True,
                                                            dict_cols_cur=dict_cols_cur)

    quote_ctx.close()

    if stockid =='':
        # if run fetch2DB directly , this codes works
        logprint('Update last fetch date as %s' % end_fetch_datetime)
        df2db.updateDB_last_fetch_date(global_module_name,end_fetch_datetime)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)
    # auto_reprocess actually call fetch2db stock by stock, so last fetch date must be update in this function instead of fetch2db.
    logprint('Update last fetch date as %s' % end_fetch_datetime)
    df2db.updateDB_last_fetch_date(global_module_name, end_fetch_datetime)

if __name__ == '__main__':
    # fetch2DB('600000')
    auto_reprocess()
