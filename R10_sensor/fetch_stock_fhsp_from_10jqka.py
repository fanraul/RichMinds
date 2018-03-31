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
    # 1.1 dividends
    dfm_db_chars_dividends = df2db.get_chars('10jqka', ['DIVIDEND'])
    dict_misc_pars_dividends = {}
    dict_misc_pars_dividends['char_origin'] = '10jqka'
    dict_misc_pars_dividends['char_freq'] = "D"
    dict_misc_pars_dividends['allow_multiple'] = 'Y'
    dict_misc_pars_dividends['created_by'] = dict_misc_pars_dividends['update_by'] = global_module_name
    dict_misc_pars_dividends['char_usage'] = 'DIVIDEND'

    # check whether db table is created.
    table_name_dividends = R50_general.general_constants.dbtables['stock_dividend_10jqka']
    df2db.create_table_by_template(table_name_dividends,table_type='stock_date_multi_value')
    dict_cols_cur_dividends = {'报告期':'nvarchar(50)',
                             '董事会日期':'datetime',
                             '股东大会预案公告日期': 'datetime',
                             '实施日期':'datetime',
                             '分红方案说明':'nvarchar(400)',
                             '送股(股)/10股': 'decimal(12,8)',
                             '转增(股)/10股': 'decimal(12,8)',
                             '派息(税前)(元)/10股': 'decimal(12,8)',
                             'A股股权登记日':'datetime',
                             'A股除权除息日':'datetime',
                             '方案进度':'nvarchar(50)',
                             '股利支付率':'decimal(10,4)',
                             '分红率':'decimal(10,4)',
                             '方案文本解析错误标识位': 'char(1)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur_dividends,
                                 list(dfm_db_chars_dividends['Char_ID']),
                                 table_name_dividends,
                                 dict_misc_pars_dividends)

    # 1.2 allotment
    dfm_db_chars_allotment = df2db.get_chars('10jqka', ['ALLOTMENT'])
    dict_misc_pars_allotment = {}
    dict_misc_pars_allotment['char_origin'] = '10jqka'
    dict_misc_pars_allotment['char_freq'] = "D"
    dict_misc_pars_allotment['allow_multiple'] = 'N'
    dict_misc_pars_allotment['created_by'] = dict_misc_pars_allotment['update_by'] = global_module_name
    dict_misc_pars_allotment['char_usage'] = 'ALLOTMENT'

    # check whether db table is created.
    table_name_allotment = R50_general.general_constants.dbtables['stock_allotment_10jqka']
    df2db.create_table_by_template(table_name_allotment,table_type='stock_date')
    dict_cols_cur_allotment = {'实际配股比例':'nvarchar(50)',
                            '配股上市日':'datetime',
                            '证监会核准公告日':'datetime',
                            '每股配股价格':'decimal(6,3)',
                            '缴款起止日':'nvarchar(50)',
                            '发审委公告日':'datetime',
                            '实际募集资金净额': 'decimal(12,0)',
                            '预案配股比例上限' :'nvarchar(50)',
                            '除权日':'datetime',
                            '股东大会公告日' :'datetime',
                            '预案募资金额上限' :'nvarchar(50)',
                            '股权登记日' :'datetime',
                            '董事会公告日' :'datetime',
                            '配股(股)/10股':'decimal(6,3)',
                            '方案文本解析错误标识位': 'char(1)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur_allotment,
                                 list(dfm_db_chars_allotment['Char_ID']),
                                 table_name_allotment,
                                 dict_misc_pars_allotment)

    # 1.3 directional issuance
    dfm_db_chars_diriss = df2db.get_chars('10jqka', ['DIR_ISS'])
    dict_misc_pars_diriss = {}
    dict_misc_pars_diriss['char_origin'] = '10jqka'
    dict_misc_pars_diriss['char_freq'] = "D"
    dict_misc_pars_diriss['allow_multiple'] = 'Y'
    dict_misc_pars_diriss['created_by'] = dict_misc_pars_diriss['update_by'] = global_module_name
    dict_misc_pars_diriss['char_usage'] = 'DIR_ISS'

    # check whether db table is created.
    table_name_diriss = R50_general.general_constants.dbtables['stock_diriss_10jqka']
    df2db.create_table_by_template(table_name_diriss, table_type='stock_date_multi_value')
    dict_cols_cur_diriss = {
                            '方案进度': 'nvarchar(50)',
                            '发行类型': 'nvarchar(50)',
                            '发行方式': 'nvarchar(50)',
                            '实际发行价格': 'decimal(6,3)',
                            '新股上市公告日': 'datetime',
                            '实际发行数量': 'decimal(13,0)',
                            '发行新股日': 'datetime',
                            '实际募资净额': 'decimal(13,0)',
                            '证监会核准公告日': 'datetime',
                            '预案发行价格': 'nvarchar(50)',
                            '发审委公告日': 'datetime',
                            '预案发行数量': 'nvarchar(50)',
                            '股东大会公告日': 'datetime',
                            '预案募资金额': 'nvarchar(50)',
                            '董事会公告日': 'datetime',
                               }
    df2db.add_new_chars_and_cols(dict_cols_cur_diriss,
                                 list(dfm_db_chars_diriss['Char_ID']),
                                 table_name_diriss,
                                 dict_misc_pars_diriss)

    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    # print(dfm_stocks)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])

        table_name_snapshot = R50_general.general_constants.dbtables['stock_fhsp_html_snapshot_10jqka']
        str_sql = "Market_ID = '%s' and Stock_ID = '%s'" %(row['Market_ID'],row['Stock_ID'])
        dfm_fhsp = df2db.get_data_from_DB(table_name_snapshot,free_conditions=str_sql)
        if len(dfm_fhsp) == 0:
            logprint('No fhsp webpage scrapped for stock %s, please double check!' %row['Stock_ID'],add_log_files='I')
            continue



        html = dfm_fhsp.iloc[0]['fhsp_html']
        soup_html = BeautifulSoup(html,"lxml")

        fetch_dividends(soup_html,row,table_name_dividends,dict_cols_cur_dividends,dict_misc_pars_dividends)
        fetch_allotment(soup_html,row,table_name_allotment,dict_cols_cur_allotment,dict_misc_pars_allotment)
        fetch_diriss(soup_html,row,table_name_diriss,dict_cols_cur_diriss,dict_misc_pars_diriss)

def fetch_diriss(soup_html,row,table_name,dict_cols_cur,dict_misc_pars):
    global ls_dfm_diriss

    dfm_stk_info = soup_parse_stock_diriss(soup_html, row['Stock_ID'])

    # TODO: error handling
    if len(dfm_stk_info) == 0:
        logprint('No stock directional issuance details can be found for stockid %s' % row['Stock_ID'])
    else:
        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)
        gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur, dateformat='%Y-%m-%d')

        ls_dfm_diriss.append(dfm_stk_info)

        del dfm_stk_info['Stock_ID']

        # gcf.dfmprint(dfm_stk_info)
        # load to DB
        df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                           row['Stock_ID'],
                                                           dfm_stk_info,
                                                           table_name,
                                                           dict_misc_pars, float_fix_decimal=3)
def soup_parse_stock_diriss(soup,stockid):
    tags_div = soup.find_all('div',id="additionprofile")

    if len(tags_div) == 0:
        return DataFrame()

    tags_tables = tags_div[0].find_all(class_="m_table pggk mt10")
    if len(tags_tables) == 0:
        return DataFrame()

    # print(tags_trs)
    ls_fhsp =[]
    ls_index =[]

    for tag_fhsp in tags_tables:
        # print(tag_fhsp)
        head_fhsp = tag_fhsp.caption.find_all('span')
        # print(head_fhsp)
        if len(head_fhsp) == 0 or head_fhsp[0].text.strip() != '已实施':    # 实施状态
            # 方案尚未实施,无法录入数据库(没有确定的key值),忽略这些记录
            continue

        dt_fhsp = {}
        dt_fhsp['Stock_ID'] = stockid
        dt_fhsp['方案进度'] = head_fhsp[0].text.strip()
        dt_fhsp['发行类型'] = head_fhsp[1].text.strip()
        dt_fhsp['发行方式'] = head_fhsp[2].text.strip()

        content_fhsp = tag_fhsp.find_all('td',class_="f12")
        # print(content_fhsp[0].span.string.strip())

        # place to exclude incorrect/useless records:
        if not diriss_validate_record_to_process(content_fhsp,stockid):
            continue

        dt_fhsp['实际发行价格'] = float(content_fhsp[0].span.string.strip()[:-1])  #31.5300元
        dt_fhsp['新股上市公告日'] = content_fhsp[1].span.string.strip()      #2007-09-04
        dt_fhsp['实际发行数量'] = parse_actual_allot_amount(content_fhsp[2].span.string.strip() )   #3.17亿股
        dt_fhsp['发行新股日'] = content_fhsp[3].span.string.strip()  #2007-08-24
        dt_fhsp['实际募资净额'] = parse_actual_allot_amount(content_fhsp[4].span.string.strip())   #99.37亿元
        dt_fhsp['证监会核准公告日'] = content_fhsp[5].span.string.strip()  #2007-08-22
        dt_fhsp['预案发行价格'] = content_fhsp[6].span.string.strip()    #--
        dt_fhsp['发审委公告日'] = content_fhsp[7].span.string.strip()     # 2000-01-10
        dt_fhsp['预案发行数量'] = content_fhsp[8].span.string.strip()     # 2000-01-10
        dt_fhsp['股东大会公告日'] = content_fhsp[9].span.string.strip()     # 2007-07-13
        dt_fhsp['预案募资金额'] = content_fhsp[10].span.string.strip()   #--
        dt_fhsp['董事会公告日'] = content_fhsp[11].span.string.strip()     #2000-01-07

    #
    #     # special_rule_for_exceptional_dividend_txt(content_fhsp[1].string.strip())
        ls_fhsp.append(dt_fhsp)

        # 新股上市公告日没有空值,故作为trans_datetime
        report_datetime = datetime.strptime(dt_fhsp['新股上市公告日'],'%Y-%m-%d')
    #
        ls_index.append(report_datetime)
    #
    if len(ls_fhsp) > 0:
        return DataFrame(ls_fhsp,index = ls_index)
        # return DataFrame(ls_fhsp)
    else:
        return DataFrame()

'''
<table class="m_table pggk mt10">
			<caption class="m_cap">
				方案进度：
				<span class="f14" style="display:inline-block;width:110px"><strong>已实施</strong></span> 
				发行类型：
				<span class="f14">公开增发</span> 
				发行方式：
				<span class="f14">网上网下定价</span>
			</caption>
			<tbody>
				<tr>
					<td class="f12">实际发行价格：<span class="f14"><strong>31.5300元</strong></span>
					</td>
					<td class="f12">新股上市公告日：<span class="f14"><strong>2007-09-04</strong></span>
					</td>
				</tr>
				<tr>
					<td class="f12">实际发行数量：<span class="f14"><strong>3.17亿股</strong></span>
					</td>
					<td class="f12">发行新股日：<span class="f14"><strong>2007-08-24</strong></span>
					</td>
				</tr>
				<tr>
					<td class="f12">实际募资净额：<span class="f14">99.37亿元</span>
					</td>
					<td class="f12">证监会核准公告日：<span class="f14">2007-08-22</span>
					</td>
				</tr>
				<tr>
					<td class="f12">预案发行价格：
						<span class="f14">--</span>
					</td>
					<td class="f12">发审委公告日：<span class="f14">2007-07-13</span>
					</td>
				</tr>
				<tr>
					<td class="f12">预案发行数量：<span class="f14">--</span>
					</td>
					<td class="f12">股东大会公告日：
						<span class="f14">2007-04-16</span>
					</td>
				</tr>
				<tr>
					<td class="f12">预案募资金额：
						<span class="f14">--</span>
					</td>
					<td class="f12">董事会公告日：<span class="f14">2007-03-20</span>
					</td>
				</tr>
			</tbody>
		</table>
'''

def diriss_validate_record_to_process(content_fhsp,stockid):
    if content_fhsp[0].span.string.strip() == '--':
        return False

    return True


def fetch_allotment(soup_html,row,table_name,dict_cols_cur,dict_misc_pars):
    global ls_dfm_allotment

    dfm_stk_info = soup_parse_stock_allotment(soup_html, row['Stock_ID'])

    # TODO: error handling
    if len(dfm_stk_info) == 0:
        logprint('No stock allotment details can be found for stockid %s' % row['Stock_ID'])
    else:
        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)
        gcf.dfm_col_type_conversion(dfm_stk_info, columns=dict_cols_cur, dateformat='%Y-%m-%d')

        ls_dfm_allotment.append(dfm_stk_info)

        del dfm_stk_info['Stock_ID']

        # gcf.dfmprint(dfm_stk_info)
        # load to DB
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_stk_info,
                                                            table_name,
                                                            dict_misc_pars,float_fix_decimal=3,
                                                            processing_mode='w_update')

def soup_parse_stock_allotment(soup,stockid):
    tags_div = soup.find_all(id="stockallotdata")

    if len(tags_div) == 0:
        return DataFrame()

    tags_tables = tags_div[0].find_all(class_="m_table pggk mt10")
    if len(tags_tables) == 0:
        return DataFrame()

    # print(tags_trs)
    ls_fhsp =[]
    ls_index =[]

    for tag_fhsp in tags_tables:

        if tag_fhsp.caption.span.text.strip() != '已实施':    # 实施状态
            # 配股方案尚未实施,无法录入数据库(没有确定的key值),忽略这些记录
            continue

        content_fhsp = tag_fhsp.find_all('td',class_="f12")
        # print(content_fhsp[0].span.string.strip())

        # place to exclude incorrect/useless records:
        # if not validate_record_to_process(content_fhsp,stockid):
        #     continue

        dt_fhsp = {}
        dt_fhsp['Stock_ID'] = stockid
        dt_fhsp['实际配股比例'] = content_fhsp[0].span.string.strip()  #10 配 2.7 股
        dt_fhsp['配股上市日'] = content_fhsp[1].span.string.strip()      #2000-02-16
        dt_fhsp['证监会核准公告日'] = content_fhsp[2].span.string.strip()    #--
        dt_fhsp['每股配股价格'] = float(content_fhsp[3].span.string.strip()[:-1])  #7.50 元
        dt_fhsp['缴款起止日'] = content_fhsp[4].span.string.strip()   #2000-01-11 到 2000-01-24
        dt_fhsp['发审委公告日'] = content_fhsp[5].span.string.strip()  #--
        dt_fhsp['实际募集资金净额'] = parse_actual_allot_amount(content_fhsp[6].span.string.strip() )   #6.25亿元
        dt_fhsp['预案配股比例上限'] = content_fhsp[7].span.string.strip()    #--
        dt_fhsp['除权日'] = content_fhsp[8].span.string.strip()     # 2000-01-10
        dt_fhsp['股东大会公告日'] = content_fhsp[9].span.string.strip()     #
        dt_fhsp['预案募资金额上限'] = content_fhsp[10].span.string.strip()   #--
        dt_fhsp['股权登记日'] = content_fhsp[11].span.string.strip()      #2000-01-07
        dt_fhsp['董事会公告日'] = content_fhsp[12].span.string.strip()     #--

    #
    #     # special_rule_for_exceptional_dividend_txt(content_fhsp[1].string.strip())
    #
    #     # parse 实际配股比例 into detail entries
        (
         dt_fhsp['配股(股)/10股'],
         dt_fhsp['方案文本解析错误标识位']) = parse_allotment_txt_to_number(dt_fhsp['实际配股比例'])
    #
        ls_fhsp.append(dt_fhsp)

        # 除权日没有空值,故使用除权日作为trans_datetime
        report_datetime = datetime.strptime(dt_fhsp['除权日'],'%Y-%m-%d')
    #
        ls_index.append(report_datetime)
    #
    if len(ls_fhsp) > 0:
        return DataFrame(ls_fhsp,index = ls_index)
    else:
        return DataFrame()


'''
			<caption class="m_cap">
				方案进度
				<a href="javascript:void(0)" class="m_more popping" targ="pg_fajd"></a>：
				<span class="f14"><strong>已实施</strong></span> 配股代码：
				<span class="f14">8002</span> 配股简称：
				<span class="f14">万科A1配</span> 配股年份：
				<span class="f14">1999年</span>
			</caption>
'''

'''
<td class="f12">实际配股比例：<span class="f14"><strong>10 配 2.7 股</strong></span></td>, 
<td class="f12">配股上市日：<span class="f14"><strong>2000-02-16</strong></span></td>, 
<td class="f12">证监会核准公告日：<span class="f14"><strong>--</strong></span></td>, 
<td class="f12">每股配股价格：<span class="f14"><strong>7.50 元</strong></span></td>, 
<td class="f12">缴款起止日：<span class="f14"> 2000-01-11 到 2000-01-24</span></td>, 
<td class="f12">发审委公告日：<span class="f14">--</span></td>, 
<td class="f12">实际募集资金净额：<span class="f14">6.25亿元</span></td>, 
<td class="f12">预案配股比例上限：<span class="f14">--</span></td>, 
<td class="f12">除权日：<span class="f14">2000-01-10</span></td>, 
<td class="f12">股东大会公告日：<span class="f14">--</span></td>, 
<td class="f12">预案募资金额上限：<span class="f14">--</span></td>, 
<td class="f12">股权登记日：<span class="f14">2000-01-07</span> </td>, 
<td class="f12">董事会公告日：<span class="f14">--</span> </td>
'''



def fetch_dividends(soup_html,row,table_name,dict_cols_cur,dict_misc_pars):
    global ls_dfm_dividend

    dfm_stk_dividend = soup_parse_stock_dividend(soup_html, row['Stock_ID'])

    # TODO: error handling
    if len(dfm_stk_dividend) == 0:
        logprint('No stock dividends details can be found for stockid %s' % row['Stock_ID'])
    else:
        # step2: format raw data into prop data type
        # gcf.dfmprint(dfm_stk_info)
        gcf.dfm_col_type_conversion(dfm_stk_dividend, columns=dict_cols_cur, dateformat='%Y-%m-%d')

        ls_dfm_dividend.append(dfm_stk_dividend)

        del dfm_stk_dividend['Stock_ID']

        # gcf.dfmprint(dfm_stk_info)
        # load to DB
        df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                           row['Stock_ID'],
                                                           dfm_stk_dividend,
                                                           table_name,
                                                           dict_misc_pars, float_fix_decimal=8)

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

    # print(tags_trs)
    ls_fhsp =[]
    ls_index =[]

    for tag_fhsp in tags_trs:
        content_fhsp = tag_fhsp.find_all('td')

        # place to exclude known incorrect/useless records:
        if not validate_record_to_process(content_fhsp,stockid):
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
        dt_fhsp['股利支付率'] = gcf.pertentage_conversion(content_fhsp[8].string.strip())
        dt_fhsp['分红率'] = gcf.pertentage_conversion(content_fhsp[9].string.strip())

        # special_rule_for_exceptional_dividend_txt(content_fhsp[1].string.strip())

        # parse 分红方案说明 into detail entries
        (dt_fhsp['送股(股)/10股'],
         dt_fhsp['转增(股)/10股'],
         dt_fhsp['派息(税前)(元)/10股'],
         dt_fhsp['方案文本解析错误标识位']) = parse_dividend_txt_to_number(dt_fhsp['分红方案说明']
                                                                ) #if dt_fhsp['方案进度'] =='实施方案' else (None,None,None,None)

        ls_fhsp.append(dt_fhsp)
        report_datetime = parse_report_date(dt_fhsp['报告期'])

        ls_index.append(report_datetime)

    if len(ls_fhsp) > 0:
        return DataFrame(ls_fhsp,columns = ['Stock_ID',	'报告期','董事会日期','股东大会预案公告日期','实施日期','分红方案说明',
                                            'A股股权登记日',	'A股除权除息日','方案进度','股利支付率','分红率',
                                            '送股(股)/10股','转增(股)/10股','派息(税前)(元)/10股','方案文本解析错误标识位'],
                                            index = ls_index)
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

def parse_report_date(str_report_date:str):
    if str_report_date.endswith('年报'):
        str_date = str_report_date[:4] + '1231'
        return datetime.strptime(str_date,'%Y%m%d')
    elif str_report_date.endswith('一季报'):
        str_date = str_report_date[:4] + '0331'
        return datetime.strptime(str_date,'%Y%m%d')
    elif str_report_date.endswith('三季报'):
        str_date = str_report_date[:4] + '0930'
        return datetime.strptime(str_date,'%Y%m%d')
    elif str_report_date.endswith('中报'):
        str_date = str_report_date[:4] + '0630'
        return datetime.strptime(str_date,'%Y%m%d')
    else:
        return datetime.strptime(str_report_date,'%Y-%m-%d')

def validate_record_to_process(content_fhsp,stockid):
    if content_fhsp[4].string.strip() == '不分配不转增':
        return False

    # an error records already identified
    # don't know why, record may be exceptional. so here you can filter found the known errors.
    if stockid == '600368' and content_fhsp[0].string.strip() == '2017年报' and content_fhsp[4].string.strip() == '10派3.5元(含税)':
        return False

    if content_fhsp[4].string.strip() == '--':
        return False

    return True

def parse_actual_allot_amount(str_amount:str):
    if str_amount.endswith('亿元') or str_amount.endswith('亿股'):
        return float(str_amount[:-2])* 100000000
    elif str_amount.endswith('万元') or str_amount.endswith('万股'):
        return float(str_amount[:-2]) * 10000
    elif str_amount == '0.00元' or str_amount == '--':
        return None
    else:
        assert 0==1, 'unknown format for fhsp数量金额转换:' + str_amount

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

def parse_allotment_txt_to_number(allot_txt:str):
    if allot_txt.startswith('10'):
        ls_pei = re.findall('配.*?([0-9.]+)',allot_txt)
        error_flg = None
        try:
            pei = float(ls_pei[0]) if ls_pei else None
        except:
            pei = None
            error_flg = 'E'
            print(allot_txt,ls_pei)
        if len(ls_pei) > 1:
            pei= None
            error_flg = 'E'
    else:
        pei = None
        error_flg = 'E'

    return pei,error_flg

def special_rule_for_exceptional_dividend_txt(s):
    if s == '10派1.01元（含税）,大股东西航集团10派0.78元（含税）':
        return '10派1.01元（含税）'
    if s == '10派2元(含税)追加方案:10送1股派1元(含税)':  #20090626
        return '10送1股派3元(含税)'
    return s

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call= fetch2DB, wait_seconds= 20)

if __name__ == '__main__':
    # fetch2DB('000002')

    auto_reprocess()

    if len(ls_dfm_dividend) > 0:
        pd.concat(ls_dfm_dividend).to_excel(get_tmp_file('dividend_10jqka.xlsx'))

    if len(ls_dfm_allotment) > 0:
        pd.concat(ls_dfm_allotment).to_excel(get_tmp_file('allotment_10jqka.xlsx'))

    if len(ls_dfm_diriss) > 0:
        pd.concat(ls_dfm_diriss).to_excel(get_tmp_file('diriss_10jqka.xlsx'))