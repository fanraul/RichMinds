import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
import json
import R50_general.advanced_helper_funcs as ahf

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


def fetch2DB(stockid:str = ''):

    # 1.init step
    # create DD tables for data store and add chars for stock structure.
    # 1.1 setup DB structure for core concept table
    dfm_db_chars_cpt = df2db.get_chars('EASTMONEY', ['CORE_CONCEPT'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'EASTMONEY'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'Y'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'CORE_CONCEPT'
    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_core_concept_eastmoney']
    df2db.create_table_by_template(table_name,table_type='stock_date_multi_value')
    dict_cols_cur_cpt = {'关键词': 'nvarchar(200)',
                          '要点内容':'nvarchar(4000)'}   #4000是nvarchar的最大值
    df2db.add_new_chars_and_cols(dict_cols_cur_cpt, list(dfm_db_chars_cpt['Char_ID']), table_name, dict_misc_pars)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' % row['Stock_ID'])
        # step1:parse webpage and get raw data
        url_stock_cpt = R50_general.general_constants.weblinks['stock_core_concept_eastmoney'] % row['MktStk_ID']
        json_stock_cpt = gcf.get_webpage(url_stock_cpt,flg_return_json=True)
        dfm_stk_cpts = soup_parse_stock_cpt(json_stock_cpt)
        if len(dfm_stk_cpts) > 0:
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_cpts,
                                                            table_name,
                                                            dict_misc_pars,
                                                            process_mode='w_check')
        else:
            logprint("Stock %s doesn't have core concept assigned" % (row['MktStk_ID']))


        # TODO: error handling
        # assert len(dfm_stk_strc) > 0, 'No stock structure details can be found for stockid %s' % row['Stock_ID']
        #
        # # step2: format raw data into prop data type
        # dfm_stk_strc['股本单位'] = '万股'
        # gcf.dfmprint(dfm_stk_strc)
        #
        # # formating
        # dfm_fmt_stk_strc = format_dfm_stk_strc(dfm_stk_strc)
        # # only one entry allowed in one day, so need to combine multiple changes in one day into one entry
        # process_duplicated_entries(dfm_fmt_stk_strc, row['Stock_ID'])
        # gcf.dfmprint(dfm_fmt_stk_strc)
        # market_id = 'SH' if row['Stock_ID'].startswith('6') else 'SZ'
        # df2db.load_dfm_to_db_by_mkt_stk_w_hist(market_id, row['Stock_ID'], dfm_fmt_stk_strc, table_name,
        #                                        dict_misc_pars,
        #                                        processing_mode='w_update')
        # time.sleep(10)



def soup_parse_stock_cpt(json_stock_cpt:str):
    dt_json_stkcpt = json.loads(json_stock_cpt)
    # print(dt_soup_stkcpt)
    ls_cpts = []
    if dt_json_stkcpt:
        ls_hxtc = dt_json_stkcpt['hxtc']
        if ls_hxtc:
            for item in ls_hxtc:
                dt_cpt = {}
                dt_cpt['关键词'] = item['gjc'].strip()
                dt_cpt['要点内容'] = item['ydnr'].strip()
                ls_cpts.append(dt_cpt)
            # print(len(dt_cpt['关键词']),',',len(dt_cpt['要点内容']))
            return DataFrame(ls_cpts)
    return DataFrame()

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    # fetch2DB('601377')
    auto_reprocess()