import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import urllib.error
from bs4 import BeautifulSoup
import re
from datetime import datetime
import gc

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

import R90_tquant.getdata as tt

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

#use Tquant module to get the data, the datasource of Tquant is cninfo.
def fetch2DB(stockid:str = ''):

    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('CNINFO', ['ALLOTMENT'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'CNINFO'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'ALLOTMENT'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_allotment_cninfo']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'股权登记日':'datetime',
                     '配股交款起止日':'nvarchar(50)',
                     '配股价':'decimal(8,2)',
                     '配股可流通部分上市日': 'datetime',
                     '配股年度':'nvarchar(50)',
                     '配股方案':'nvarchar(400)',
                     '除权基准日':'datetime',
                     '配股(股)/10股': 'decimal(12,8)',
                     '方案文本解析错误标识位': 'char(1)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)


    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    # print(dfm_stocks)
    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        # url_stock_info = R50_general.general_constants.weblinks['stock_dividend_cninfo'] %{'market_id':gcf.market_id_conversion_for_cninfo(row['Stock_ID']),
        #                                                                                    'stock_id':row['Stock_ID']}
        # # print(url_link)
        # try:
        #    soup_stock_info = gcf.get_webpage(url_stock_info)
        #
        # # cninfo will raise error 404 if no dividend information exist for certain stock, need capture it and skip this stock
        # except urllib.error.HTTPError as e:
        #     if e.code == 404:
        #         logprint('No stock dividends details can be found for stockid %s' % row['Stock_ID'])
        #         continue
        #     else:
        #         raise e
        # dfm_stk_info = soup_parse_stock_dividend(soup_stock_info)



        # 直接使用Tquant的函数
        dfm_allot = tt.get_allotment(row['Stock_ID'])

        # TODO: error handling
        if len(dfm_allot) == 0:
            logprint('No stock allotment details can be found for stockid %s' %row['Stock_ID'])
            return

        dfm_allot_formated = format_stock_allotment(dfm_allot)

        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)
        gcf.dfm_col_type_conversion(dfm_allot_formated, columns=dict_cols_cur,dateformat='%Y%m%d')
        # only one entry allowed in one day, so need to combine multiple changes in one day into one entry
        # 配股记录应该不会发生同一天有多条记录的问题吧...
        # process_duplicated_entries(dfm_stk_info,row['Stock_ID'])
        # gcf.dfmprint(dfm_stk_info)
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_allot_formated,
                                                            table_name,
                                                            dict_misc_pars,float_fix_decimal=8,
                                                            processing_mode='w_update')

def format_stock_allotment(dfm_allot):
    ls_dfm_allot = []
    ls_index = []
    for index, row in dfm_allot.iterrows():
        dt_allot = {}
        dt_allot['股权登记日'] = row['股权登记日'].strip()
        dt_allot['配股交款起止日'] = row['配股交款起止日'].strip()
        dt_allot['配股价'] = float(row['配股价'].strip())
        dt_allot['除权基准日'] = row['除权基准日'].strip()
        dt_allot['配股可流通部分上市日'] = row['配股可流通部分上市日'].strip()
        dt_allot['配股年度'] = row['配股年度'].strip()
        dt_allot['配股方案'] = special_rule_for_exceptional_allotment_txt(row['配股方案'].strip())
        (dt_allot['配股(股)/10股'],
         dt_allot['方案文本解析错误标识位']) = parse_allotment_txt_to_number(row['配股方案'].strip())

        ls_dfm_allot.append(dt_allot)
        ls_index.append(datetime.strptime(dt_allot['除权基准日'], '%Y%m%d'))

    return DataFrame(ls_dfm_allot,index = ls_index)

# def soup_parse_stock_dividend(soup):
#     tags_table = soup.findAll('table')
#     if len(tags_table) >= 3:
#         tags_fhsp = (tags_table[2]).findAll('tr')
#
#         ls_fhsp = []
#         ls_index = []
#         for tag_fhsp in tags_fhsp[1:]:
#             content_fhsp = tag_fhsp.findAll('td')
#             if content_fhsp[1].string.strip():
#                 dt_fhsp = {}
#                 dt_fhsp['分红年度'] = content_fhsp[0].string.strip()
#                 dt_fhsp['分红方案'] = special_rule_for_exceptional_dividend_txt(content_fhsp[1].string.strip())
#                 dt_fhsp['股权登记日'] = content_fhsp[2].string.strip()
#                 dt_fhsp['除权基准日'] = content_fhsp[3].string.strip()
#                 dt_fhsp['红股上市日'] = content_fhsp[4].string.strip()
#                 (dt_fhsp['送股(股)/10股'],
#                  dt_fhsp['转增(股)/10股'] ,
#                  dt_fhsp['派息(税前)(元)/10股'],
#                  dt_fhsp['方案文本解析错误标识位']) = parse_dividend_txt_to_number(dt_fhsp['分红方案'])
#
#                 ls_fhsp.append(dt_fhsp)
#                 ls_index.append(datetime.strptime(dt_fhsp['除权基准日'], '%Y%m%d'))
#
#         return DataFrame(ls_fhsp,index = ls_index)
#     else:
#         return DataFrame()

'''
配股年度	配股方案	配股价	股权登记日	除权基准日	配股交款起止日	配股可流通部分上市日
1998年度	10配2.727股，每股7.5元（实施方案）	7.5	20000107	20000110	20000111-20000124	20000216
1996年度	10配2.37股，每股4.5元（实施方案）	4.5	19970711	19970714	19970716-19970729	19970822
1990年度	10配5股，每股4.4元（实施方案）	4.4	19910531	19910601	19910601-	 
'''

def parse_allotment_txt_to_number(allot_txt:str):
    if allot_txt.startswith('10'):
        ls_pei = re.findall('配.*?([0-9.]+)',allot_txt)
        error_flg = None

        if len(ls_pei) != 1:
            pei = None
            error_flg = 'E'

        try:
            pei = float(ls_pei[0])
        except:
            pei = None
            error_flg = 'E'
            print(allot_txt,ls_pei)
    else:
        pei = None
        error_flg = 'E'

    return pei,error_flg

def special_rule_for_exceptional_allotment_txt(s):
    # if s == '10派1.01元（含税）,大股东西航集团10派0.78元（含税）':
    #     return '10派1.01元（含税）'
    # if s == '10派2元(含税)追加方案:10送1股派1元(含税)':  #20090626
    #     return '10送1股派3元(含税)'
    return s

# def process_duplicated_entries(dfm_stk_info:DataFrame,stockid):
#     dfm_duplicated = dfm_stk_info[dfm_stk_info.duplicated(['股权登记日'])]
#     # print(dfm_duplicated)
#     dfm_stk_info.drop_duplicates('股权登记日',inplace=True)
#     for index, row in dfm_duplicated.iterrows():
#         dfm_stk_info.loc[index,'分红年度'] = add_considering_None(dfm_stk_info.loc[index]['分红年度'],row['分红年度'])
#         dfm_stk_info.loc[index,'分红方案'] = dfm_stk_info.loc[index]['分红方案'] + '|' + row['分红方案']
#         if dfm_stk_info.loc[index]['方案文本解析错误标识位'] !='E':
#             if row['方案文本解析错误标识位'] == 'E':
#                 dfm_stk_info.loc[index, '方案文本解析错误标识位'] = 'E'
#                 dfm_stk_info.loc[index, '派息(税前)(元)/10股'] = None
#                 dfm_stk_info.loc[index, '转增(股)/10股'] = None
#                 dfm_stk_info.loc[index, '送股(股)/10股'] = None
#             else:
#                 dfm_stk_info.loc[index,'派息(税前)(元)/10股'] = add_considering_None(dfm_stk_info.loc[index]['派息(税前)(元)/10股'],row['派息(税前)(元)/10股'])
#                 dfm_stk_info.loc[index,'转增(股)/10股'] = add_considering_None(dfm_stk_info.loc[index]['转增(股)/10股'] , row['转增(股)/10股'])
#                 dfm_stk_info.loc[index,'送股(股)/10股'] = add_considering_None(dfm_stk_info.loc[index]['送股(股)/10股'] , row['送股(股)/10股'])
#         logprint('Stock %s 股权登记日 %s 记录合并到主记录中. %s' %(stockid,row['股权登记日'],tuple(row)))

# def add_considering_None(f1,f2,strsplitter='|'):
#     if f1 and f2:
#         if type(f1) == type('a'):
#             return f1 + strsplitter +f2
#         else:
#             return f1 + f2
#     if f1 and not f2:
#         return f1
#     if not f1 and f2:
#         return f2
#     return None

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call= fetch2DB, wait_seconds= 20)

if __name__ == '__main__':
    # fetch2DB('000002')
    fetch2DB()
    # auto_reprocess()