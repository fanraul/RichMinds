import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import re

from datetime import datetime

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

"""
this program is run after Tquant update all the stock list ,then check whether stock name is changed or not.
if changed, update the record in DD_stock_name_change_hist_Tquant
"""

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

def update_stock_name_changes(stockid:str = ''):
    """
    get stock names in stock_basic_info table and compare with records with DD_stock_name_change_hist_Tquant table
    if different name found, update name changes
    :return:
    """
    # init step
    # step1.1: get current stock list with stock name currently
    dfm_stocks = df2db.get_cn_stocklist(stockid)[['Market_ID', 'Stock_ID', 'Stock_Name']]

    #get chars for name change hist
    dfm_db_chars = df2db.get_chars('Tquant', ['NAME_HIST'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'Tquant'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] ='N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] =global_module_name
    dict_misc_pars['char_usage'] = 'NAME_HIST'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['name_hist_Tquant']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'证券简称': 'nvarchar(50)',
                     '原证券简称': 'nvarchar(50)'}
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    # step1.2 : get latest snapshot of stock change hist table
    dfm_stock_name_changes_db = df2db.get_lastest_snapshot_from_mtkstk_hist_table('DD_stock_name_change_hist_Tquant')[
                                    ['Market_ID', 'Stock_ID', '证券简称']]
    if len(dfm_stock_name_changes_db) == 0:
        # first_processing
        dfm_stocks['原证券简称'] = '---'
        dfm_stocks.rename(columns = {'Stock_Name':'证券简称'},inplace=True)
        df2db.load_dfm_to_db_cur(dfm_stocks,['Market_ID','Stock_ID'],'DD_stock_name_change_hist_Tquant',dict_misc_pars,'wo_update')
    else:
        dfm_stock_name_changes_cur = pd.merge(dfm_stocks,dfm_stock_name_changes_db,how='left',on = ['Market_ID','Stock_ID'])
        dfm_stock_name_changes_cur.fillna(value='---',inplace = True)
        dfm_stock_name_changes_cur.rename(columns = {'证券简称':'原证券简称'},inplace = True)
        dfm_stock_name_changes_cur.rename(columns = {'Stock_Name':'证券简称'},inplace = True)
        dfm_stock_name_changes_cur = dfm_stock_name_changes_cur[dfm_stock_name_changes_cur.证券简称 != dfm_stock_name_changes_cur.原证券简称]
        gcf.dfmprint(dfm_stock_name_changes_cur)
        df2db.load_dfm_to_db_cur(dfm_stock_name_changes_cur,['Market_ID','Stock_ID'],'DD_stock_name_change_hist_Tquant',dict_misc_pars,'w_update')

if __name__ ==  '__main__':
    update_stock_name_changes()