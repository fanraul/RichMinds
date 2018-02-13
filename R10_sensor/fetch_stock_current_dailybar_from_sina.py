import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

def fetch2DB(stockid:str):
    '''
    the function used in 3 ways:
    single test mode: call this function with one stockid, then you fetch this stockid's data,mainly for test
    try-run mode: call this function without parameter, it fetch all stocks' data, but it is obselete now! since
                  now I use 3rd mode mainly.
    auto-reprocess mode: call this funcion one by one for all stockid,it is used in dry-run and real job model.

    in this case, it is better get last trading date at module2 level instead of function level. so move last trading date
    ,last_fetch_date at module level.
    :param stockid:
    :return:
    '''
    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist

    dfm_db_chars = df2db.get_chars('SINA', ['DAILYBAR'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'SINA'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DAILYBAR'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_dailybar_sina']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'open':'decimal(12,4)',
                     'close':'decimal(12,4)',
                     'high':'decimal(12,4)',
                     'low':'decimal(12,4)',
                     'vol':'decimal(15,2)',
                     'amount': 'decimal(15,2)',
                     '前收盘':'decimal(12,4)',
                     '数据刷新时间':'datetime',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        dfm_stk_info = gcf.get_stock_current_trading_info_sina(row['MktStk_ID'],return_format='close_daily_fetch')
        # TODO: error handling
        if len(dfm_stk_info) == 0:
            logprint('No stock dailybars can be found for stockid %s' %row['Stock_ID'])
        else:
            dfm_stk_info = dfm_stk_info[list(dict_cols_cur.keys())]
            # gcf.dfmprint(dfm_stk_info)
            # step2: format raw data into prop data type
            gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur,dateformat='%Y-%m-%d %H:%M:%S')
            gcf.dfmprint(dfm_stk_info)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_stk_info, table_name,
                                                                dict_misc_pars,
                                                                processing_mode='w_update',float_fix_decimal=2,partial_ind= True)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    # fetch2DB('300712')
    auto_reprocess()

'''
大秦铁路,8.800,8.790,9.030,9.060,8.700,9.030,9.040,109367311,971205411.000,73530,9.030,895919,9.020,555200,9.010,29700,9.000,137700,8.990,256000,9.040,772531,9.050,1108457,9.060,269100,9.070,248000,9.080,2017-11-17,15:00:00,00
0：”大秦铁路”，股票名字；
1：”27.55″，今日开盘价；
2：”27.25″，昨日收盘价；
3：”26.91″，当前价格；
4：”27.55″，今日最高价；
5：”26.20″，今日最低价；
6：”26.91″，竞买价，即“买一”报价；
7：”26.92″，竞卖价，即“卖一”报价；
8：”22114263″，成交的股票数，由于股票交易以一百股为基本单位，所以在使用时，通常把该值除以一百；
9：”589824680″，成交金额，单位为“元”，为了一目了然，通常以“万元”为成交金额的单位，所以通常把该值除以一万；
10：”4695″，“买一”申请4695股，即47手；
11：”26.91″，“买一”报价；
12：”57590″，“买二”
13：”26.90″，“买二”
14：”14700″，“买三”
15：”26.89″，“买三”
16：”14300″，“买四”
17：”26.88″，“买四”
18：”15100″，“买五”
19：”26.87″，“买五”
20：”3100″，“卖一”申报3100股，即31手；
21：”26.92″，“卖一”报价
(22, 23), (24, 25), (26,27), (28, 29)分别为“卖二”至“卖四的情况”
30：”2008-01-11″，日期；
31：”15:05:32″，时间；
'''