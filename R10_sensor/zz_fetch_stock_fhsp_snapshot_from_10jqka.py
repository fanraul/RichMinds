import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import urllib.error
from bs4 import BeautifulSoup
import re
from datetime import datetime
import gc

import time
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint,get_tmp_file
import R50_general.dfm_to_table_common as df2db


global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


# 10jqka会block ip,所以用这个程序先把网页保存到sql server中,然后在进行后续的处理

def fetch2DB(stockid:str = ''):

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
                html_stock_fhsp = gcf.get_webpage(url_stock_info,flg_return_rawhtml = True)

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


def parse_html_from_DB(stockid:str = ''):
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        str_sql = "Market_ID = '%s' and Stock_ID = '%s'" %(row['Market_ID'],row['Stock_ID'])
        dfm_fhsp = df2db.get_data_from_DB('YY_stock_fhsp_snapshot_10jqka',free_conditions=str_sql)
        if len(dfm_fhsp) > 0:
            html = dfm_fhsp.iloc[0]['fhsp_html'].encode('UTF-8',errors='ignore')
            soup = BeautifulSoup(html,"lxml")
            dfm_stk_dividend = soup_parse_stock_dividend(soup, row['Stock_ID'])

def soup_parse_stock_dividend(soup,stockid):
    tags_table = soup.find_all(id ='bonus_table')

    if len(tags_table) == 0:
        return DataFrame()

    tags_tbody = tags_table[0].find_all('tbody')
    if len(tags_tbody) == 0:
        return DataFrame()

    tags_trs = tags_tbody[0].find_all('tr')
    if len(tags_trs) == 0:
        return DataFrame()

    print(tags_trs)
    ls_fhsp =[]

    for tag_fhsp in tags_trs:
        content_fhsp = tag_fhsp.find_all('td')
        if content_fhsp[4].string.strip() =='不分配不转增':
            continue

        dt_fhsp = {}
        dt_fhsp['Stock_ID'] = stockid
        dt_fhsp['报告期'] = content_fhsp[0].string.strip()
        dt_fhsp['董事会日期'] = content_fhsp[1].string.strip()
        dt_fhsp['股东大会预案公告日期'] = content_fhsp[2].string.strip()
        dt_fhsp['实施日期'] = content_fhsp[3].string.strip()
        dt_fhsp['分红方案说明'] = content_fhsp[4].string.strip()
        dt_fhsp['A股股权登记日'] = content_fhsp[5].string.strip()
        dt_fhsp['A股除权除息日'] = content_fhsp[6].string.strip()
        dt_fhsp['方案进度'] = content_fhsp[7].string.strip()
        dt_fhsp['股利支付率'] = content_fhsp[8].string.strip()
        dt_fhsp['分红率'] = content_fhsp[9].string.strip()

        # special_rule_for_exceptional_dividend_txt(content_fhsp[1].string.strip())

        # parse 分红方案说明 into detail entries
        # (dt_fhsp['送股(股)/10股'],
        #  dt_fhsp['转增(股)/10股'],
        #  dt_fhsp['派息(税前)(元)/10股'],
        #  dt_fhsp['方案文本解析错误标识位']) = parse_dividend_txt_to_number(dt_fhsp['分红方案'])

        ls_fhsp.append(dt_fhsp)
        # ls_index.append(datetime.strptime(dt_fhsp['股权登记日'], '%Y%m%d'))

        # return DataFrame(ls_fhsp,index = ls_index)

    if len(ls_fhsp) > 0:
        return DataFrame(ls_fhsp,columns = ['Stock_ID',	'报告期','董事会日期','股东大会预案公告日期','实施日期','分红方案说明',
                                            'A股股权登记日',	'A股除权除息日','方案进度','股利支付率','分红率'])
    else:
        return DataFrame()


if __name__ == '__main__':
    # fetch2DB('600638')
    parse_html_from_DB('600638')

    # auto_reprocess()
