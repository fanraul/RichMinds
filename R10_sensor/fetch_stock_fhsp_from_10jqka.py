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
from R50_general.general_helper_funcs import logprint,get_tmp_file
import R50_general.dfm_to_table_common as df2db


global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


# 程序有解析方案和处理同一天多条分红信息的功能,顾不使用Tquant的函数获得分红信息,保持原有的处理方式.
# program use 股权登记日 as key due to some stocks fh doesn't have 除权基准日 info,maybe wrong record need to fix
# to simplily scrap the web and loaded into DB, use 股权登记日 as key can ealisy avoid this problem,
# before use, user need to double check the  除权基准日 and fix it if required.

ls_dfm_dividend =[]
ls_dfm_allotment =[]
ls_dfm_diriss =[]

def fetch2DB(stockid:str = ''):

    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('10jqka', ['DIVIDEND'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = '10jqka'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DIVIDEND'

    # check whether db table is created.
    # table_name = R50_general.general_constants.dbtables['stock_dividend_cninfo']
    # df2db.create_table_by_template(table_name,table_type='stock_date')
    # dict_cols_cur = {'分红年度':'nvarchar(50)',
    #                  '分红方案':'nvarchar(400)',
    #                  '红股上市日':'datetime',
    #                  '股权登记日':'datetime',
    #                  '除权基准日':'datetime',
    #                  '送股(股)/10股': 'decimal(12,8)',
    #                  '转增(股)/10股': 'decimal(12,8)',
    #                  '派息(税前)(元)/10股': 'decimal(12,8)',
    #                  '方案文本解析错误标识位': 'char(1)',
    #                 }
    # df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)


    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    # print(dfm_stocks)

    global ls_dfm_dividend,ls_dfm_allotment,ls_dfm_diriss

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        url_stock_info = R50_general.general_constants.weblinks['stock_fhsp_10jqka'] %row['Stock_ID']
        # print(url_link)

        try:
            soup_stock_info = gcf.get_webpage(url_stock_info)
        # special handing for this webpage!!!
        # 10jqka will raise error 401:Unauthorized if it detect web scrapping
        # if detected by 10jqka, web page shows: 401 Authorization Required
        except urllib.error.HTTPError as e:
            if e.code == 401:
                logprint('web scrap is detected by 10jqka, web page blocked',add_log_files='I')
                import sys
                sys.exit(1)
            else:
                raise e

        dfm_stk_dividend = soup_parse_stock_dividend(soup_stock_info,row['Stock_ID'])

        # TODO: error handling
        if len(dfm_stk_dividend) == 0:
            logprint('No stock fhsp details can be found for stockid %s' %row['Stock_ID'])
        else:
            ls_dfm_dividend.append(dfm_stk_dividend)
            # step2: format raw data into prop data type
            # gcf.dfmprint(dfm_stk_info)
            # gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur,dateformat='%Y%m%d')
            # only one entry allowed in one day, so need to combine multiple changes in one day into one entry
            # process_duplicated_entries(dfm_stk_info,row['Stock_ID'])
            # gcf.dfmprint(dfm_stk_info)
            # df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_stk_info, table_name,
            #                                                     dict_misc_pars,
            #                                                     processing_mode='w_update')

        # dfm_stk_diriss = soup_parse_stock_diriss(soup_stock_info)  # directional add-issuance
        # dfm_stk_allotment = soup_parse_stock_allotment(soup_stock_info)  # directional add-issuance


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

'''
<table class="m_table m_hl mt15" id="bonus_table">
	<thead>
		<tr>
			<th>报告期</th>
			<th>董事会日期</th>
			<th>股东大会预案公告日期</th>
			<th>实施日期</th>
			<th>分红方案说明</th>
			<th>A股股权登记日</th>
			<th content="除权除息日是指上市公司发放股息红利的日子，股权登记日下一个交易日即是除权除息日。">A股除权除息日</th>
			<th>方案进度</th>
			<th content="股利支付率，也称股息发放率，是指净收益中股利所占的比重。它反映公司的股利分配政策和股利支付能力。">股利支付率</th>
			<th content="分红率指在一个考察期（通常为12个月的时间）内，股票的每股分红净额除以公告实施当天的收盘价格的百分比。">分红率</th>
		</tr>
	</thead>
	<tbody>
		<tr class="J_pageritem ">
			<td style="width:75px;" class="tc">2017中报</td>
			<td style="width:75px;" class="tc">2017-08-29</td>
			<td style="width:75px;" class="tc">--</td>
			<td style="width:75px;" class="tc">2017-09-20</td>
			<td style="width:100px;" class="tl">10派1.20元(含税)</td>
			<td style="width:75px;" class="tc">2017-09-25</td>
			<td style="width:75px;" class="tc">2017-09-26</td>
			<td style="width:75px;" class="tl">实施方案</td>
			<td style="width:45px;">12.63%</td>
			<td style="width:45px;">--</td>
		</tr>
'''

def parse_dividend_txt_to_number(divid_txt:str):
    if divid_txt.startswith('10'):
        ls_zhuan = re.findall('转.*?([0-9.]+)',divid_txt)
        ls_pai = re.findall('派.*?([0-9.]+)',divid_txt)
        ls_song = re.findall('送.*?([0-9.]+)',divid_txt)
        error_flg = None
        try:
            pai = float(ls_pai[0]) if ls_pai else None
            song = float(ls_song[0]) if ls_song else None
            zhuan = float(ls_zhuan[0]) if ls_zhuan else None
        except:
            pai,song,zhuan = None,None,None
            error_flg = 'E'
            print(divid_txt,ls_pai,ls_song,ls_zhuan)
        if len(ls_pai) > 1 or len(ls_song) >1 or len(ls_zhuan) >1:
            pai, song, zhuan = None, None, None
            error_flg = 'E'
    else:
        pai, song, zhuan = None, None, None
        error_flg = 'E'

    return song,zhuan,pai,error_flg

def special_rule_for_exceptional_dividend_txt(s):
    if s == '10派1.01元（含税）,大股东西航集团10派0.78元（含税）':
        return '10派1.01元（含税）'
    if s == '10派2元(含税)追加方案:10送1股派1元(含税)':  #20090626
        return '10送1股派3元(含税)'
    return s

def process_duplicated_entries(dfm_stk_info:DataFrame,stockid):
    dfm_duplicated = dfm_stk_info[dfm_stk_info.duplicated(['股权登记日'])]
    # print(dfm_duplicated)
    dfm_stk_info.drop_duplicates('股权登记日',inplace=True)
    for index, row in dfm_duplicated.iterrows():
        dfm_stk_info.loc[index,'分红年度'] = add_considering_None(dfm_stk_info.loc[index]['分红年度'],row['分红年度'])
        dfm_stk_info.loc[index,'分红方案'] = dfm_stk_info.loc[index]['分红方案'] + '|' + row['分红方案']
        if dfm_stk_info.loc[index]['方案文本解析错误标识位'] !='E':
            if row['方案文本解析错误标识位'] == 'E':
                dfm_stk_info.loc[index, '方案文本解析错误标识位'] = 'E'
                dfm_stk_info.loc[index, '派息(税前)(元)/10股'] = None
                dfm_stk_info.loc[index, '转增(股)/10股'] = None
                dfm_stk_info.loc[index, '送股(股)/10股'] = None
            else:
                dfm_stk_info.loc[index,'派息(税前)(元)/10股'] = add_considering_None(dfm_stk_info.loc[index]['派息(税前)(元)/10股'],row['派息(税前)(元)/10股'])
                dfm_stk_info.loc[index,'转增(股)/10股'] = add_considering_None(dfm_stk_info.loc[index]['转增(股)/10股'] , row['转增(股)/10股'])
                dfm_stk_info.loc[index,'送股(股)/10股'] = add_considering_None(dfm_stk_info.loc[index]['送股(股)/10股'] , row['送股(股)/10股'])
        logprint('Stock %s 股权登记日 %s 记录合并到主记录中. %s' %(stockid,row['股权登记日'],tuple(row)))

def add_considering_None(f1,f2,strsplitter='|'):
    if f1 and f2:
        if type(f1) == type('a'):
            return f1 + strsplitter +f2
        else:
            return f1 + f2
    if f1 and not f2:
        return f1
    if not f1 and f2:
        return f2
    return None

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call= fetch2DB, wait_seconds= 20)

if __name__ == '__main__':
    fetch2DB('600638')

    # auto_reprocess()
    if len(ls_dfm_dividend) > 0:
        pd.concat(ls_dfm_dividend).to_excel(get_tmp_file('dividend_10jqka.xlsx'))

    if len(ls_dfm_allotment) > 0:
        pd.concat(ls_dfm_allotment).to_excel(get_tmp_file('allotment_10jqka.xlsx'))

    if len(ls_dfm_diriss) > 0:
        pd.concat(ls_dfm_diriss).to_excel(get_tmp_file('diriss_10jqka.xlsx'))