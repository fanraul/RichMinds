import json
from datetime import datetime

from pandas import DataFrame

import R50_general.advanced_helper_funcs as ahf
import R50_general.dfm_to_table_common as df2db
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint, parse_chinese_uom, floatN, intN

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


def fetch2DB(stockid:str = ''):
    # 1.init step
    # create DD tables for data store and add chars for stock structure.
    # 1.1 setup DB structure for core concept table
    dict_misc_pars_sh_num = gcf.set_dict_misc_pars('EASTMONEY','D','N',global_module_name,'SHAREHOLDER10')   #股东人数
    dict_misc_pars_tensh_tradable = gcf.set_dict_misc_pars('EASTMONEY','D','Y',global_module_name,'SHAREHOLDER20') #十大流通股东
    dict_misc_pars_tensh = gcf.set_dict_misc_pars('EASTMONEY','D','Y',global_module_name,'SHAREHOLDER30') #十大股东
    dict_misc_pars_tensh_changes = gcf.set_dict_misc_pars('EASTMONEY','D','Y',global_module_name,'SHAREHOLDER40') #十大股东持股变动
    dict_misc_pars_funds = gcf.set_dict_misc_pars('EASTMONEY','D','Y',global_module_name,'SHAREHOLDER50') #基金持股
    dict_misc_pars_nontradable_rls =  gcf.set_dict_misc_pars('EASTMONEY','D','N',global_module_name,'SHAREHOLDER60')    #限售解禁

    # check whether db table is created.
    # 股东人数
    dfm_db_chars_sh_sum = df2db.get_chars('EASTMONEY', ['SHAREHOLDER10'])
    table_name_sh_num = R50_general.general_constants.dbtables['stock_shareholder_number_eastmony']
    df2db.create_table_by_template(table_name_sh_num,table_type='stock_date')
    dict_cols_sh_num = {'股东人数': 'int',
                         '较上期变化比例': 'decimal(10,4)',
                         '筹码集中度': 'nvarchar(50)',
                         '股价':'decimal(10,2)',
                         '前十大股东持股合计比例':'decimal(10,4)',
                         '前十大流通股东持股合计比例':'decimal(10,4)',
                         }
    df2db.add_new_chars_and_cols(dict_cols_sh_num, list(dfm_db_chars_sh_sum['Char_ID']), table_name_sh_num, dict_misc_pars_sh_num)

    # 十大流通股东
    dfm_db_chars_tensh_tradable = df2db.get_chars('EASTMONEY', ['SHAREHOLDER20'])
    table_name_tensh_tradable = R50_general.general_constants.dbtables['stock_top_ten_tradable_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_tensh_tradable, table_type='stock_date_multi_value')
    dict_cols_tensh_tradable = { '名次': 'int',
                                 '股东名称':'nvarchar(500)',
                                 '股东性质':'nvarchar(50)',
                                 '股份类型':'nvarchar(50)',
                                 '持股数':'bigint',
                                 '占总流通股本持股比例':'decimal(10,4)',
                                 '增减股数':'nvarchar(50)',  # 值可能为'不变','新进'
                                 '变动比例':'decimal(10,4)',
                                 }
    df2db.add_new_chars_and_cols(dict_cols_tensh_tradable, list(dfm_db_chars_tensh_tradable['Char_ID']), table_name_tensh_tradable, dict_misc_pars_tensh_tradable)

    #十大股东
    dfm_db_chars_tensh = df2db.get_chars('EASTMONEY', ['SHAREHOLDER30'])
    table_name_tensh = R50_general.general_constants.dbtables['stock_top_ten_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_tensh, table_type='stock_date_multi_value')
    dict_cols_tensh = { '名次': 'int',
                     '股东名称':'nvarchar(500)',
                     '股份类型':'nvarchar(50)',
                     '持股数':'bigint',
                     '占总流通股本持股比例':'decimal(10,4)',
                     '增减股数':'nvarchar(50)',  # 值可能为'不变','新进'
                     '变动比例':'decimal(10,4)',
                     }
    df2db.add_new_chars_and_cols(dict_cols_tensh, list(dfm_db_chars_tensh['Char_ID']), table_name_tensh, dict_misc_pars_tensh)

    # 十大股东持股变动
    dfm_db_chars_tensh_changes = df2db.get_chars('EASTMONEY', ['SHAREHOLDER40'])
    table_name_tensh_changes = R50_general.general_constants.dbtables['stock_top_ten_shareholder_shares_changes_eastmoney']
    df2db.create_table_by_template(table_name_tensh_changes, table_type='stock_date_multi_value')
    dict_cols_tensh_changes = {'变动时间':'datetime',
                               '名次': 'int',
                                '股东名称':'nvarchar(500)',
                                '股份类型':'nvarchar(50)',
                                '持股数':'bigint',
                                '占总流通股本持股比例':'decimal(10,4)',
                                '增减股数':'nvarchar(50)',  # 值可能为'不变','新进'
                                '增减股占原股东持股比例':'decimal(10,4)',
                                '变动原因':'nvarchar(50)',
                               }

    df2db.add_new_chars_and_cols(dict_cols_tensh_changes, list(dfm_db_chars_tensh_changes['Char_ID']),
                                 table_name_tensh_changes, dict_misc_pars_tensh_changes)

    # 基金持股
    dfm_db_chars_funds = df2db.get_chars('EASTMONEY', ['SHAREHOLDER50'])
    table_name_funds = R50_general.general_constants.dbtables['stock_fund_shareholder_eastmoney']
    df2db.create_table_by_template(table_name_funds, table_type='stock_date_multi_value')
    dict_cols_funds = {'名次': 'int',
                        '基金代码':'nvarchar(50)',
                        '基金名称':'nvarchar(500)',
                        '持股数':'bigint',
                        '持仓市值':'decimal(18,2)',
                        '占总股本比':'decimal(10,4)',
                        '占流通比':'decimal(10,4)',
                        '占净值比':'decimal(10,4)',
                       }

    df2db.add_new_chars_and_cols(dict_cols_funds, list(dfm_db_chars_funds['Char_ID']),
                                 table_name_funds, dict_misc_pars_funds)

    # 限售解禁
    dfm_db_chars_nontradable_rls = df2db.get_chars('EASTMONEY', ['SHAREHOLDER60'])
    table_name_nontradable_rls = R50_general.general_constants.dbtables['stock_nontradable_shares_release_eastmoney']
    df2db.create_table_by_template(table_name_nontradable_rls, table_type='stock_date')
    dict_cols_nontradable_rls = {'解禁股数': 'bigint',
                                 '股票类型':'nvarchar(50)',
                                 '解禁股占总股本比例':'decimal(10,4)',
                                 '解禁股占流通股本比例':'decimal(10,4)',
                                 }

    df2db.add_new_chars_and_cols(dict_cols_nontradable_rls, list(dfm_db_chars_nontradable_rls['Char_ID']),
                                 table_name_nontradable_rls, dict_misc_pars_nontradable_rls)


    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' % row['Stock_ID'])
        # step1:parse webpage and get raw data
        url_stock_sh = R50_general.general_constants.weblinks['stock_shareholder_eastmoney'] % row['MktStk_ID']
        json_stock_sh = gcf.get_webpage(url_stock_sh,flg_return_json=True)

        dfm_stk_sh_sum = json_parse_stock_sh10(json_stock_sh)


        if len(dfm_stk_sh_sum) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_sh_sum, columns=dict_cols_sh_num)
            # print(dfm_stk_sh_sum.dtypes)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_sh_sum,
                                                            table_name_sh_num,dict_misc_pars_sh_num,partial_ind=True)
        else:
            logprint("Stock %s doesn't have shareholder number info" % (row['MktStk_ID']))

        dfm_stk_tensh_tradable = json_parse_stock_sh20(json_stock_sh)
        if len(dfm_stk_tensh_tradable) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_tensh_tradable, columns=dict_cols_tensh_tradable)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_stk_tensh_tradable,
                                                            table_name_tensh_tradable,dict_misc_pars_tensh_tradable,partial_ind=True)
        else:
            logprint("Stock %s doesn't have ten tradable shareholders info" % (row['MktStk_ID']))

        dfm_stk_tensh = json_parse_stock_sh30(json_stock_sh)
        if len(dfm_stk_tensh) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_tensh, columns=dict_cols_tensh)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                               row['Stock_ID'],
                                                               dfm_stk_tensh,
                                                               table_name_tensh,
                                                               dict_misc_pars_tensh, partial_ind=True)
        else:
            logprint("Stock %s doesn't have ten shareholders info" % (row['MktStk_ID']))

        dfm_stk_tensh_changes = json_parse_stock_sh40(json_stock_sh)
        if len(dfm_stk_tensh_changes) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_tensh_changes, columns=dict_cols_tensh_changes)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                               row['Stock_ID'],
                                                               dfm_stk_tensh_changes,
                                                               table_name_tensh_changes,
                                                               dict_misc_pars_tensh_changes, partial_ind=True)
        else:
            logprint("Stock %s doesn't have ten shareholders share changes info" % (row['MktStk_ID']))

        dfm_stk_funds = json_parse_stock_sh50(json_stock_sh)
        if len(dfm_stk_funds) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_funds, columns=dict_cols_funds)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                               row['Stock_ID'],
                                                               dfm_stk_funds,
                                                               table_name_funds,
                                                               dict_misc_pars_funds, partial_ind=True)
        else:
            logprint("Stock %s doesn't have funds shareholders info" % (row['MktStk_ID']))

        dfm_stk_nontradable_rls = json_parse_stock_sh60(json_stock_sh)
        if len(dfm_stk_nontradable_rls) > 0:
            gcf.dfm_col_type_conversion(dfm_stk_nontradable_rls, columns=dict_cols_nontradable_rls)
            df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'],
                                                               row['Stock_ID'],
                                                               dfm_stk_nontradable_rls,
                                                               table_name_nontradable_rls,
                                                               dict_misc_pars_nontradable_rls, partial_ind=False)
        else:
            logprint("Stock %s doesn't have funds shareholders info" % (row['MktStk_ID']))

        # TODO: error handling

def json_parse_stock_sh10(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    ls_sh10 = []    #股东人数
    ls_sh10_index = []

    if dt_json_stksh:
        ls_gdrs = dt_json_stksh['gdrs']
        for item in ls_gdrs:
            item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
            dt_sh10 = {}
            dt_sh10['股东人数'] = parse_chinese_uom(item['gdrs'])
            dt_sh10['较上期变化比例'] = floatN(item['gdrs_jsqbh'], '/', 100)
            dt_sh10['筹码集中度'] = item['cmjzd']
            dt_sh10['股价'] = floatN(item['gj'])
            dt_sh10['前十大股东持股合计比例'] = floatN(item['qsdgdcghj'], '/', 100)
            dt_sh10['前十大流通股东持股合计比例'] = floatN(item['qsdltgdcghj'], '/', 100)
            ls_sh10.append(dt_sh10)
            ls_sh10_index.append(datetime.strptime(item['rq'],'%Y-%m-%d'))
            """
            "rq": "2017-09-30",          Trans_Datetime
			"gdrs": "3.49万",            '股东人数'
			"gdrs_jsqbh": "-10.28",      '较上期变化比例'
			"rjltg": "4.5万",
			"rjltg_jsqbh": "11.46",
			"cmjzd": "非常集中",          '筹码集中度'
			"gj": "16.08",                '股价'
			"rjcgje": "72万",
			"qsdgdcghj": "65.09",         '前十大股东持股合计比例'
			"qsdltgdcghj": "42.95"        '前十大流通股东持股合计比例'
            """
        return DataFrame(ls_sh10,index = ls_sh10_index)
    else:
        return DataFrame()

def json_parse_stock_sh20(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    # print(dt_soup_stkcpt)
    ls_sh10 = []    #十大流通股东
    ls_sh10_index = []
    if dt_json_stksh:
        ls_sdltgd_l1 = dt_json_stksh['sdltgd']
        for item_l1 in ls_sdltgd_l1:
            ls_sdltgd_l2 = item_l1['sdltgd']
            for item in ls_sdltgd_l2:
                item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
                dt_sh10 = {}
                dt_sh10['名次'] = intN(item['mc'])
                dt_sh10['股东名称'] = item['gdmc']
                dt_sh10['股东性质'] = item['gdxz']
                dt_sh10['股份类型'] = item['gflx']
                dt_sh10['持股数'] = intN(item['cgs'])
                dt_sh10['占总流通股本持股比例'] = pertentage(item['zltgbcgbl'])
                dt_sh10['增减股数'] = item['zj']
                dt_sh10['变动比例'] = pertentage(item['bdbl'])
                ls_sh10.append(dt_sh10)
                ls_sh10_index.append(datetime.strptime(item['rq'],'%Y-%m-%d'))
                """
                        "rq": "2017-09-30",
                        "mc": "1",                          '名次': 'int',
                        "gdmc": "三花控股集团有限公司",       '股东名称':'nvarchar(50)',
                        "gdxz": "其它",                       '股东性质':'nvarchar(50)',
                        "gflx": "A股",                       '股份类型':'nvarchar(50)',
                        "cgs": "788,374,733",                   '持股数':'int',
                        "zltgbcgbl": "50.41%",                '占总流通股本持股比例':'decimal(10,6)',
                        "zj": "不变",                         '增减股数':'nvarchar(50)',
                        "bdbl": "--",                        '变动比例':'decimal(10,6)',  
                """
        return DataFrame(ls_sh10,index = ls_sh10_index)
    else:
        return DataFrame()


def json_parse_stock_sh30(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    # print(dt_soup_stkcpt)
    ls_sh10 = []    #十大股东
    ls_sh10_index = []
    if dt_json_stksh:
        ls_sdgd_l1 = dt_json_stksh['sdgd']
        for item_l1 in ls_sdgd_l1:
            ls_sdgd_l2 = item_l1['sdgd']
            for item in ls_sdgd_l2:
                item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
                dt_sh10 = {}
                dt_sh10['名次'] = intN(item['mc'])
                dt_sh10['股东名称'] = item['gdmc']
                dt_sh10['股份类型'] = item['gflx']
                dt_sh10['持股数'] = intN(item['cgs'])
                dt_sh10['占总流通股本持股比例'] = pertentage(item['zltgbcgbl'])
                dt_sh10['增减股数'] = item['zj']
                dt_sh10['变动比例'] = pertentage(item['bdbl'])
                ls_sh10.append(dt_sh10)
                ls_sh10_index.append(datetime.strptime(item['rq'],'%Y-%m-%d'))
                """
                        "rq": "2017-09-30",
                        "mc": "1",                          '名次': 'int',
                        "gdmc": "三花控股集团有限公司",       '股东名称':'nvarchar(50)',
                        "gflx": "A股",                       '股份类型':'nvarchar(50)',
                        "cgs": "788,374,733",                   '持股数':'int',
                        "zltgbcgbl": "50.41%",                '占总流通股本持股比例':'decimal(10,6)',
                        "zj": "不变",                         '增减股数':'nvarchar(50)',
                        "bdbl": "--",                        '变动比例':'decimal(10,6)',  
                """
        return DataFrame(ls_sh10,index = ls_sh10_index)
    else:
        return DataFrame()

def json_parse_stock_sh40(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    ls_sh10 = []    #持股变化
    ls_sh10_index = []

    if dt_json_stksh:
        ls_sdgdcgbd = dt_json_stksh['sdgdcgbd']
        if ls_sdgdcgbd == None:
            return DataFrame()
        for item in ls_sdgdcgbd:
            item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
            dt_sh10 = {}
            dt_sh10['变动时间'] = item['bdsj']
            dt_sh10['名次'] = intN(item['mc'])
            dt_sh10['股东名称'] = item['gdmc']
            dt_sh10['股份类型'] = item['gflx']
            dt_sh10['持股数'] = intN(item['cgs'])
            dt_sh10['占总流通股本持股比例'] = pertentage(item['zzgbcgbl'])
            dt_sh10['增减股数'] = item['cj']
            dt_sh10['增减股占原股东持股比例'] = pertentage(item['cjgzygdcgbl'])
            dt_sh10['变动原因'] = item['bdyy']
            ls_sh10.append(dt_sh10)
            ls_sh10_index.append(datetime.strptime(item['bdsj'],'%Y-%m-%d'))
            """
			"bdsj": "2017-09-19",               '变动时间': 'datetime',
			"mc": "1",                          '名次': 'int',
			"gdmc": "三花控股集团有限公司",       '股东名称': 'nvarchar(50)',
			"gflx": "流通A股",                     '股份类型': 'nvarchar(50)',
			"cgs": "788,374,733.00",                '持股数': 'bigint',
			"zzgbcgbl": "37.18%",               '占总流通股本持股比例': 'decimal(10,4)',
			"cj": "--",                         '增减股数': 'nvarchar(50)',  # 值可能为'不变','新进'
			"cjgzygdcgbl": "--",                '增减股占原股东持股比例': 'decimal(10,4)',
			"bdyy": "增发上市"                  '变动原因': 'nvarchar(50)',
            """

        return DataFrame(ls_sh10,index = ls_sh10_index)
    else:
        return DataFrame()

def json_parse_stock_sh50(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    ls_sh10 = []    #基金股东
    ls_sh10_index = []
    if dt_json_stksh:
        ls_jjcg_l1 = dt_json_stksh['jjcg']
        if ls_jjcg_l1 == None:
            return DataFrame()
        for item_l1 in ls_jjcg_l1:
            ls_jjcg_l2 = item_l1['jjcg']
            for item in ls_jjcg_l2:
                item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
                dt_sh10 = {}
                dt_sh10['名次'] = intN(item['id'])
                dt_sh10['基金代码'] = item['jjdm']
                dt_sh10['基金名称'] = item['jjmc']
                dt_sh10['持股数'] = intN(item['cgs'])
                dt_sh10['持仓市值'] = floatN(item['cgsz'])
                dt_sh10['占总股本比'] = pertentage(item['zzgbb'])
                dt_sh10['占流通比'] = pertentage(item['zltb'])
                dt_sh10['占净值比'] = pertentage(item['zjzb'])
                ls_sh10.append(dt_sh10)
                ls_sh10_index.append(datetime.strptime(item_l1['rq'],'%Y-%m-%d'))
                """
			"rq": "2017-09-30",
			"jjcg": [
				{
					"id": "",                       '名次': 'int',
					"jjdm": "020001",               '基金代码': 'nvarchar(50)',
					"jjmc": "国泰金鹰增长混合",         '基金名称': 'nvarchar(50)',
					"cgs": "12,488,801.00",         '持股数': 'bigint',
					"cgsz": "200,819,920.08",       '持仓市值': 'decimal(18,2)',
					"zzgbb": "0.59%",               '占总股本比': 'decimal(10,4)',
					"zltb": "0.78%",                '占流通比': 'decimal(10,4)',
					"zjzb": "--",                   '占净值比': 'decimal(10,4)',
					"order": "12488801"
                """

        return DataFrame(ls_sh10,index = ls_sh10_index)
    else:
        return DataFrame()

def json_parse_stock_sh60(json_stock_sh:str):
    dt_json_stksh= json.loads(json_stock_sh)
    ls_sh10 = []    #限售解禁
    ls_sh10_index = []

    if dt_json_stksh:
        ls_xsjj = dt_json_stksh['xsjj']
        if not ls_xsjj:
            return DataFrame()
        for item in ls_xsjj:
            item = {x:y.strip() if y != '--' and y else None for (x,y) in item.items()}
            dt_sh10 = {}
            dt_sh10['解禁股数'] = parse_chinese_uom(item['jjsl'])
            dt_sh10['解禁股占总股本比例'] = pertentage(item['jjgzzgbbl'])
            dt_sh10['解禁股占流通股本比例'] = pertentage(item['jjgzltgbbl'])
            dt_sh10['股票类型'] = item['gplx']

            ls_sh10.append(dt_sh10)
            ls_sh10_index.append(datetime.strptime(item['jjsj'],'%Y-%m-%d'))
            """
			"jjsj": "2018-07-23",               
			"jjsl": "2.09亿",                    '解禁股数': 'bigint',
			"jjgzzgbbl": "9.85%",                '解禁股占总股本比例': 'decimal(10,4)',
			"jjgzltgbbl": "13.35%",             '解禁股占流通股本比例': 'decimal(10,4)',
			"gplx": "定向增发机构配售股份"        '股票类型': 'nvarchar(50)',
            """

        # if same transaction datetime has multiple lines, need to combine them into one line.
        ls_sh10_index_new = []
        ls_sh10_new =[]
        for i in range(len(ls_sh10)):
            if ls_sh10_index[i] not in ls_sh10_index_new:
                ls_sh10_new.append(ls_sh10[i])
                ls_sh10_index_new.append(ls_sh10_index[i])
            else:
                # find the index of the duplicated entry
                j = ls_sh10_index_new.index(ls_sh10_index[i])
                ls_sh10_new[j]['解禁股数'] += ls_sh10[i]['解禁股数']
                ls_sh10_new[j]['解禁股占总股本比例'] += ls_sh10[i]['解禁股占总股本比例']
                ls_sh10_new[j]['解禁股占流通股本比例'] += ls_sh10[i]['解禁股占流通股本比例']
                ls_sh10_new[j]['股票类型'] += '|'+ls_sh10[i]['股票类型']

        return DataFrame(ls_sh10_new,index = ls_sh10_index_new)
    else:
        return DataFrame()


def pertentage(p:str):
    if not p:
        return None
    if p.endswith('%'):
        return float(p[:-1])/100
    return float(p)/100

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call=fetch2DB, wait_seconds=60)

if __name__ == '__main__':
    fetch2DB('002195')
    # auto_reprocess()