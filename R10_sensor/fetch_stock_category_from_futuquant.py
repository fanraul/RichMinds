"""
得到所有板块及其板块下的所有的股票清单
"""
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

def fetch2DB():
    # 1.1 get category code list
    # 板块分类	标识
    # 所有板块	“ALL”
    # 行业分类	“INDUSTRY”
    # 地域分类	“REGION”
    # 概念分类	“CONCEPT”
    ls_market = ['HK',"SH","US"]   # SZ and SH return the same date, no difference
                                               # HK_FUTURE and HK return the same data
    # get catg type <> 'ALL'
    ls_catg_type = ['INDUSTRY','REGION','CONCEPT']
    ls_dfm_catgs =[]
    for market in ls_market:
        for catg_type in ls_catg_type:
            dfm_catg = get_plate_list_futu(api_ip,api_port,market,catg_type)
            dfm_catg['Catg_Type'] = catg_type
            dfm_catg['Catg_Origin'] = 'FUTUQUANT'
            ls_dfm_catgs.append(dfm_catg)
    dfm_catgs_not_all = pd.concat(ls_dfm_catgs,ignore_index=True)

    ls_dfm_catgs =[]
    # get catg type = 'ALL'
    for market in ls_market:
        dfm_catg = get_plate_list_futu(api_ip, api_port, market, 'ALL')
        dfm_catg['Catg_Type'] = 'ALL'
        dfm_catg['Catg_Origin'] = 'FUTUQUANT'
        ls_dfm_catgs.append(dfm_catg)
    dfm_catg_all = pd.concat(ls_dfm_catgs,ignore_index=True)

    # remove duplicated
    dfm_catg_all_wo_duplicate = gcf.dfm_A_minus_B(dfm_catg_all,dfm_catgs_not_all,['code','plate_name'])
    dfm_cur_catg = pd.concat([dfm_catgs_not_all,dfm_catg_all_wo_duplicate],ignore_index=True)

    dfm_cur_catg.rename(columns={'code':'Catg_Id',
                                 'plate_name':'Catg_Name',
                                 'plate_id':'Catg_Reference',},inplace= True)

    # all nvarchar, no need to convert
    # gcf.dfm_col_type_conversion(dfm_stocklist, columns=dict_cols_cur, dateformat='%Y-%m-%d')

    key_cols = ['Catg_Origin','Catg_Type','Catg_Id','Catg_Name']

    table_name = R50_general.general_constants.dbtables['category']

    df2db.dfm_to_db_insert_or_update(dfm_cur_catg, key_cols, table_name, global_module_name,
                                     process_mode='w_update')

    # dfm_cur_catg.to_excel('catg_list.xls')

    # 1.2 update category and related stocks info into DB
    dfm_db_chars = df2db.get_chars('FUTUQUANT', ['CATG'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'FUTUQUANT'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'Y'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'CATG'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_category_stocks_futuquant']
    df2db.create_table_by_template(table_name, table_type='catg_date_multi_value_futuquant')
    dict_cols_cur = {'MtkStk_ID': 'nvarchar(50)',
                     'Market_ID':'nvarchar(50)',
                     'Stock_ID':'nvarchar(50)',
                     'lot_size': 'int',
                     'owner_market': 'nvarchar(10)',
                     'stock_child_type': 'nvarchar(10)',
                     'stock_type':'nvarchar(10)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    dfm_cur_catg.drop_duplicates(subset=['Catg_Id'],inplace=True)
    ls_dfm_catgstocks_all =[]
    for index, row in dfm_cur_catg.iterrows():
        ret, dfm_catg_stocks = gcf.get_plate_stocks_futuquant(api_ip, api_port, row['Catg_Id'])
        if ret == RET_ERROR:
            logprint('Failed to get stocks under catg %s. Err message: %s' %(row['Catg_Id']+':'+row['Catg_Name'],dfm_catg_stocks))
        else:
            dfm_catg_stocks['Market_ID'] = dfm_catg_stocks.apply(lambda s:gcf.get_mkt_stk_futuquant(s['code'])[0] ,axis =1)
            dfm_catg_stocks['Stock_ID'] = dfm_catg_stocks.apply(lambda s:gcf.get_mkt_stk_futuquant(s['code'])[1] ,axis =1)
            dfm_catg_stocks.rename(columns={'code':'MtkStk_ID'},inplace=True)
            del dfm_catg_stocks['stock_name']
            gcf.dfm_col_type_conversion(dfm_catg_stocks, columns=dict_cols_cur)

            dt_key_cols = {'Catg_Id': row['Catg_Id']}
            # gcf.dfmprint(dfm_catg_stocks)
            df2db.load_dfm_to_db_multi_value_by_key_cols_cur(dt_key_cols, dfm_catg_stocks, table_name, dict_misc_pars,
                                                             process_mode='w_check',float_fix_decimal = 4)
            ls_dfm_catgstocks_all.append(dfm_catg_stocks)
    pd.concat(ls_dfm_catgstocks_all).to_excel('catg_stocks_all.xls')


def get_plate_list_futu(ip,port,market,plate_class):
    quote_ctx= OpenQuoteContext(ip, port)
    ret_code, ret_data = quote_ctx.get_plate_list(market, plate_class)
    if ret_code == RET_ERROR:
        assert 0==1,"Can't get category list for market %s, type %s.Err msg: %s" %(market, plate_class,ret_data)
    quote_ctx.close()
    return ret_data

if __name__ == "__main__":
    # gcf.dfmprint(get_plate_list_futu(api_ip, api_port, 'HK','ALL'))
    # gcf.dfmprint(gcf.get_index_stocks_futuquant(api_ip,api_port,'HK.BK1129'))
    # fetch2DB()
    print(global_module_name)