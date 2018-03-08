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

def fetch2DB(stockid:str):
    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('futuquant', ['ADJUST'])  #ADJINFO: 复权数据
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'futuquant'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'ADJUST'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_adjinfo_futuquant']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'split_ratio':'decimal(10,5)',  #split_ratio：拆合股比例 double，例如，对于5股合1股为1/5，对于1股拆5股为5/1
                     'per_cash_div':'decimal(10,5)',  #per_cash_div：每股派现；double
                     'per_share_div_ratio':'decimal(10,5)',  #per_share_div_ratio：每股送股比例； double
                     'per_share_trans_ratio':'decimal(10,5)',  #per_share_trans_ratio：每股转增股比例； double
                     'allotment_ratio':'decimal(10,5)',  #allotment_ratio： 每股配股比例；double
                     'allotment_price':'decimal(10,5)',  #allotment_price：配股价；double
                     'stk_spo_ratio':'decimal(10,5)',  #stk_spo_ratio： 增发比例：double
                     'stk_spo_price':'decimal(10,5)',  #stk_spo_price 增发价格：double
                     'forward_adj_factorA':'decimal(10,5)',  #forward_adj_factorA：前复权因子A；double
                     'forward_adj_factorB':'decimal(10,5)',  #forward_adj_factorB：前复权因子B；double
                     'backward_adj_factorA':'decimal(10,5)',  #backward_adj_factorA：后复权因子A；double
                     'backward_adj_factorB':'decimal(10,5)',  #backward_adj_factorB：后复权因子B；double
                     }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    # buildup connection with futuquant server
    quote_ctx = OpenQuoteContext(api_ip, api_port)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        mtkstk_id = row['Market_ID'] + '.' + row['Stock_ID']

        ret, dfm_autype = quote_ctx.get_autype_list([row['Market_ID']+'.'+row['Stock_ID']])

        if ret == RET_ERROR:
            logprint('Error during fetch %s autype , error message: %s' % (mtkstk_id, dfm_autype), add_log_files='I')
            continue

        if len(dfm_autype) == 0:
            logprint('No stock autype can be found for stockid %s' %row['Stock_ID'])
            continue

        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)

        # futu autype API 返回值不应有重复值,如有重复值,报错
        s_duplicated = dfm_autype.duplicated(['code','ex_div_date'])
        if len(s_duplicated[s_duplicated==True]) > 0:
            assert 0==1,'Fatal error: duplicated entry find for stockid %s' % mtkstk_id

        dfm_autype['Trans_Datetime'] = dfm_autype.apply(lambda s: datetime.strptime(s['ex_div_date'],'%Y-%m-%d'),axis=1)

        # dfm_autype.rename(columns = {'change_rate':'PCHG',
        #                            'volume':'vol',
        #                            'turnover':'amount'},inplace=True)
        dfm_autype.set_index('Trans_Datetime',inplace=True)

        dfm_autype.drop(['ex_div_date','code'],axis =1,inplace = True)

        gcf.dfm_col_type_conversion(dfm_autype, columns=dict_cols_cur)
        # gcf.dfmprint(dfm_stk_info)
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_autype, table_name,
                                                            dict_misc_pars,
                                                            processing_mode='w_update',float_fix_decimal= 5,
                                                            partial_ind= False,
                                                            dict_cols_cur=dict_cols_cur)

    quote_ctx.close()


def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    fetch2DB('600000')
    # auto_reprocess()
