import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint, parse_chinese_uom, floatN, intN
import R50_general.dfm_to_table_common as df2db
import json
import R50_general.advanced_helper_funcs as ahf

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


def fetch2DB(stockid:str = ''):

    # 1.init step
    # create DD tables for data store and add chars for stock structure.
    # 1.1 setup DB structure for core concept table
    dfm_db_chars = df2db.get_chars('EASTMONEY', ['BASIC_INFO'],['D','NA'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'EASTMONEY'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'BASIC_INFO'
    # check whether db table is created.
    table_name_comp = R50_general.general_constants.dbtables['stock_company_general_info_eastmoney']
    df2db.create_table_by_template(table_name_comp,table_type='stock_date')
    table_name_issue = R50_general.general_constants.dbtables['stock_company_issuance_info_eastmoney']
    df2db.create_table_by_template(table_name_issue,table_type='stock_wo_date')


    dict_cols_cur_comp = {'公司名称': 'nvarchar(200)',
                         '英文名称': 'nvarchar(200)',
                         '曾用名': 'nvarchar(200)',
                         'A股代码': 'nvarchar(200)',
                         'A股简称': 'nvarchar(200)',
                         'B股代码': 'nvarchar(200)',
                         'B股简称': 'nvarchar(200)',
                         'H股代码': 'nvarchar(200)',
                         'H股简称': 'nvarchar(200)',
                         '证券类别': 'nvarchar(200)',
                         '所属行业': 'nvarchar(200)',
                         '总经理': 'nvarchar(200)',
                         '法人代表': 'nvarchar(200)',
                         '董秘': 'nvarchar(200)',
                         '董事长': 'nvarchar(200)',
                         '证券事务代表': 'nvarchar(200)',
                         '独立董事': 'nvarchar(200)',
                         '联系电话': 'nvarchar(200)',
                         '电子信箱': 'nvarchar(200)',
                         '传真': 'nvarchar(200)',
                         '公司网址': 'nvarchar(200)',
                         '办公地址': 'nvarchar(200)',
                         '注册地址': 'nvarchar(200)',
                         '区域': 'nvarchar(200)',
                         '邮政编码': 'nvarchar(200)',
                         '注册资本(元)': 'decimal(18,2)',
                         '工商登记': 'nvarchar(200)',
                         '雇员人数': 'nvarchar(200)',
                         '管理人员人数': 'nvarchar(200)',
                         '律师事务所': 'nvarchar(200)',
                         '会计师事务所': 'nvarchar(200)',
                         '公司简介': 'nvarchar(4000)',
                         '经营范围': 'nvarchar(4000)',
                         }   #4000是nvarchar的最大值


    df2db.add_new_chars_and_cols(dict_cols_cur_comp, list(dfm_db_chars['Char_ID']), table_name_comp, dict_misc_pars)
    dict_cols_cur_issue = {'成立日期' : 'datetime',
                            '上市日期' : 'datetime',
                            '发行市盈率(倍)' : 'decimal(10,2)',
                            '网上发行日期' : 'datetime',
                            '发行方式' : 'nvarchar(200)',
                            '每股面值(元)' : 'decimal(10,2)',
                            '发行量(股)' : 'bigint',
                            '每股发行价(元)' : 'decimal(10,2)',
                            '发行费用(元)' : 'decimal(18,2)',
                            '发行总市值(元)' : 'decimal(18,2)',
                            '募集资金净额(元)' : 'decimal(18,2)',
                        }
    dict_misc_pars['char_freq'] = "NA"
    df2db.add_new_chars_and_cols(dict_cols_cur_issue, list(dfm_db_chars['Char_ID']), table_name_issue, dict_misc_pars)
    '''
            "gsmc": "中国石化齐鲁股份有限公司",         公司名称
			"ywmc": "Sinopec Qilu Company Ltd.",      英文名称
			"cym": "--",                              曾用名
			"agdm": "--",                             A股代码
			"agjc": "--",                             A股简称
			"bgdm": "--",                             B股代码
			"bgjc": "--",                             B股简称
			"hgdm": "--",                             H股代码
			"hgjc": "--",                             H股简称
			"zqlb": "--",                             证券类别
			"sshy": "--",                             所属行业
			"zjl": "王浩水",                           总经理
			"frdb": "张瑞生",                          法人代表
			"dm": "李风安",                            董秘
			"dsz": "张瑞生",                           董事长
			"zqswdb": "鲍伟松",                        证券事务代表
			"dlds": "张鸣华,任辉,匡永泰",               独立董事
			"lxdh": "0533-3583728",                    联系电话
			"dzxx": "qlsh600002@126.com",              电子信箱
			"cz": "0533-3583718",                       传真
			"gswz": "www.qilu.com.cn",                  公司网址
			"bgdz": "山东省淄博市高新技术产业开发区",      办公地址
			"zcdz": "山东省淄博市高新技术产业开发区",      注册地址
			"qy": "山东",                               区域
			"yzbm": "255086",                           邮政编码
			"zczb": "19.5亿",                            注册资本(元)
			"gsdj": "1000001002901",                    工商登记
			"gyrs": "0",                                雇员人数
			"glryrs": "19",                             管理人员人数
			"lssws": "北京市嘉源律师事务所",              律师事务所
			"kjssws": "毕马威华振会计师事务所",            会计师事务所
			"gsjj": "--",                                   公司简介
			"jyfw": "石油化工产品及无机化工...。"         经营范围  

    '''

    '''
			"clrq": "1998-03-18",               成立日期
			"ssrq": "1998-04-08",               上市日期
			"fxsyl": "13",                      发行市盈率(倍)
			"wsfxrq": "1998-03-03",             网上发行日期
			"fxfs": "网下定价发行",               发行方式
			"mgmz": "1.00",                     每股面值(元)
			"fxl": "3.50亿",                     发行量(股)
			"mgfxj": "5.00",                    每股发行价(元)
			"fxfy": "3086万",                    发行费用(元)
			"fxzsz": "17.5亿",                   发行总市值(元)
			"mjzjje": "17.2亿",                  募集资金净额(元)
			"srkpj": "5.47",
			"srspj": "5.25",
			"srhsl": "40.67%",
			"srzgj": "5.48",
			"wxpszql": "--",
			"djzql": "2.58%"
    '''
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' % row['Stock_ID'])
        # step1:parse webpage and get raw data
        url_stock_info = R50_general.general_constants.weblinks['stock_general_info_eastmoney'] % row['MktStk_ID']
        json_stock_info = gcf.get_webpage(url_stock_info,flg_return_json=True)

        dfm_stk_comp = soup_parse_stock_comp(json_stock_info)
        if len(dfm_stk_comp) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_comp, columns=dict_cols_cur_comp)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_comp,
                                                            table_name_comp,
                                                            dict_misc_pars,
                                                            process_mode='w_check',
                                                            float_fix_decimal=2)
        else:
            logprint("Stock %s doesn't have company general info" % (row['MktStk_ID']))

        dfm_stk_issue = soup_parse_stock_issue(json_stock_info)
        if len(dfm_stk_issue) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_issue, columns=dict_cols_cur_issue)

            df2db.load_dfm_to_db_single_value_by_mkt_stk_wo_datetime(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_issue,
                                                            table_name_issue,
                                                            dict_misc_pars,
                                                            process_mode='w_check',
                                                            float_fix_decimal=2)
        else:
            logprint("Stock %s doesn't have company issuance info" % (row['MktStk_ID']))



def soup_parse_stock_comp(json_stock_info:str):
    if json_stock_info:
        dt_json_stkinfo = json.loads(json_stock_info)
        dt_result = dt_json_stkinfo.get('Result')
        if dt_result:
             dt_jbzl = dt_result.get('jbzl')
             if dt_jbzl:
                ls_comps = []
                dt_comp = {}
                dt_comp['公司名称'] = dt_jbzl['gsmc'].strip()
                dt_comp['英文名称'] = dt_jbzl['ywmc'].strip()
                dt_comp['曾用名'] = dt_jbzl['cym'].strip()
                dt_comp['A股代码'] = dt_jbzl['agdm'].strip()
                dt_comp['A股简称'] = dt_jbzl['agjc'].strip()
                dt_comp['B股代码'] = dt_jbzl['bgdm'].strip()
                dt_comp['B股简称'] = dt_jbzl['bgjc'].strip()
                dt_comp['H股代码'] = dt_jbzl['hgdm'].strip()
                dt_comp['H股简称'] = dt_jbzl['hgjc'].strip()
                dt_comp['证券类别'] = dt_jbzl['zqlb'].strip()
                dt_comp['所属行业'] = dt_jbzl['sshy'].strip()
                dt_comp['总经理'] = dt_jbzl['zjl'].strip()
                dt_comp['法人代表'] = dt_jbzl['frdb'].strip()
                dt_comp['董秘'] = dt_jbzl['dm'].strip()
                dt_comp['董事长'] = dt_jbzl['dsz'].strip()
                dt_comp['证券事务代表'] = dt_jbzl['zqswdb'].strip()
                dt_comp['独立董事'] = dt_jbzl['dlds'].strip()
                dt_comp['联系电话'] = dt_jbzl['lxdh'].strip()
                dt_comp['电子信箱'] = dt_jbzl['dzxx'].strip()
                dt_comp['传真'] = dt_jbzl['cz'].strip()
                dt_comp['公司网址'] = dt_jbzl['gswz'].strip()
                dt_comp['办公地址'] = dt_jbzl['bgdz'].strip()
                dt_comp['注册地址'] = dt_jbzl['zcdz'].strip()
                dt_comp['区域'] = dt_jbzl['qy'].strip()
                dt_comp['邮政编码'] = dt_jbzl['yzbm'].strip()
                dt_comp['注册资本(元)'] = parse_chinese_uom(dt_jbzl['zczb'].strip())
                dt_comp['工商登记'] = dt_jbzl['gsdj'].strip()
                dt_comp['雇员人数'] = dt_jbzl['gyrs'].strip()
                dt_comp['管理人员人数'] = dt_jbzl['glryrs'].strip()
                dt_comp['律师事务所'] = dt_jbzl['lssws'].strip()
                dt_comp['会计师事务所'] = dt_jbzl['kjssws'].strip()
                dt_comp['公司简介'] = dt_jbzl['gsjj'].strip()
                dt_comp['经营范围'] = dt_jbzl['jyfw'].strip()
                ls_comps.append(dt_comp)
                return DataFrame(ls_comps)
    return DataFrame()

def soup_parse_stock_issue(json_stock_info:str):
    if json_stock_info:
        dt_json_stkinfo = json.loads(json_stock_info)
        dt_result = dt_json_stkinfo.get('Result')
        if dt_result:
             dt_fxxg = dt_result.get('fxxg')
             if dt_fxxg:
                ls_issue = []
                dt_issue = {}
                dt_issue['成立日期'] = dt_fxxg['clrq'].strip()
                dt_issue['上市日期'] = dt_fxxg['ssrq'].strip()
                dt_issue['发行市盈率(倍)'] = floatN(dt_fxxg['fxsyl'].strip())
                dt_issue['网上发行日期'] = dt_fxxg['wsfxrq'].strip()
                dt_issue['发行方式'] = dt_fxxg['fxfs'].strip()
                dt_issue['每股面值(元)'] = floatN(dt_fxxg['mgmz'].strip())
                dt_issue['发行量(股)'] = parse_chinese_uom(dt_fxxg['fxl'].strip())
                dt_issue['每股发行价(元)'] = floatN(dt_fxxg['mgfxj'].strip())
                dt_issue['发行费用(元)'] = parse_chinese_uom(dt_fxxg['fxfy'].strip())
                dt_issue['发行总市值(元)'] = parse_chinese_uom(dt_fxxg['fxzsz'].strip())
                dt_issue['募集资金净额(元)'] = parse_chinese_uom(dt_fxxg['mjzjje'].strip())
                ls_issue.append(dt_issue)
                return DataFrame(ls_issue)
    return DataFrame()

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    # fetch2DB('002913')
    auto_reprocess()