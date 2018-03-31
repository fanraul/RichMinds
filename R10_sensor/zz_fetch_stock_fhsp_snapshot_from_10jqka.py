import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import urllib.error
from bs4 import BeautifulSoup
import re
from datetime import datetime

import time
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint,get_tmp_file
import R50_general.dfm_to_table_common as df2db


global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


# 10jqka会block ip,所以用这个程序先把网页保存到sql server中,然后在进行后续的处理
# 目前测试看,10jqka不会马上block你的ip,而是一段时间之后才进行block,一次block大约2个小时.
# 有时候又不会block ip.

def fetch2DB(stockid:str = ''):

    table_name = R50_general.general_constants.dbtables['stock_fhsp_html_snapshot_10jqka']
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    # print(dfm_stocks)

    dict_misc_pars = {}
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        url_stock_info = R50_general.general_constants.weblinks['stock_fhsp_10jqka'] %row['Stock_ID']
        # print(url_link)

        while True:
            try:
                html_stock_fhsp = gcf.get_webpage(url_stock_info)

            # special handing for this webpage!!!
            # 10jqka will raise error 401:Unauthorized if it detect web scrapping
            # if detected by 10jqka, web page shows: 401 Authorization Required
            except urllib.error.HTTPError as e:
                if e.code == 401:
                    logprint('web scrap is detected by 10jqka, web page blocked,wait 10 mins and retry!')
                    time.sleep(600)
                    continue
                else:
                    raise e

            break


        dfm_fhsp = DataFrame([{'fhsp_html':str(html_stock_fhsp)}])
        df2db.load_dfm_to_db_single_value_by_mkt_stk_wo_datetime(row['Market_ID'],
                                                                 row['Stock_ID'],
                                                                 dfm_fhsp,
                                                                 'YY_stock_fhsp_snapshot_10jqka',
                                                                 dict_misc_pars,
                                                                 process_mode='w_check')

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call= fetch2DB, wait_seconds= 20)

if __name__ == '__main__':
    # fetch2DB('600638')
    # parse_html_from_DB('600638')

    auto_reprocess()
