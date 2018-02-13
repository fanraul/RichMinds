import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

import R50_general.general_constants
from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

"""
this program parse the stock basic info page from qq :
http://stock.finance.qq.com/corp1/profile.php?zqdm=%(stock_id)s


it extract below 3 types datas 
-change hist, especially name change log
-stock category info
"""


def fetch2DB(mode:str = ''):
    """
    fetch stock name history and update into DB
    :param mode: if mode = 'update_log" then delete all entries in YY_stock_changes_qq,otherwise only update name changes
    :return:
    """
    # init step
    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist()
    #only for test purpose,use one stock to test.
    # dfm_stocks = df2db.get_cn_stocklist('000155')

    #get chars for name change hist
    dfm_db_chars = df2db.get_chars('QQ', ['NAME_HIST'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'QQ'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] ='N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] ='fetch_stock_change_record_from_qq'
    dict_misc_pars['char_usage'] = 'NAME_HIST'

    #get chars for stock category
    dfm_db_chars_catg = df2db.get_chars('QQ', ['CATG'])
    dict_misc_pars_catg = {}
    dict_misc_pars_catg['char_origin'] = 'QQ'
    dict_misc_pars_catg['char_freq'] = "D"
    dict_misc_pars_catg['allow_multiple'] ='Y'
    dict_misc_pars_catg['created_by'] = dict_misc_pars_catg['update_by'] ='fetch_stock_change_record_from_qq'
    dict_misc_pars_catg['char_usage'] = 'CATG'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['name_hist_qq']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    table_name_concept = R50_general.general_constants.dbtables['category_qq']
    df2db.create_table_by_template(table_name_concept,table_type='stock_date_multi_value')

    # get db category list
    dfm_db_catg = df2db.get_catg('QQ')
    set_db_catg = df2db.dfm_value_to_set(dfm_db_catg,['Catg_Name'])
    print(set_db_catg)

    ls_changes =[]
    # step2:loop at stock list and fetch and save to DB
    for item in dfm_stocks['Stock_ID']:         # get column Stock_ID from dataframe
        # Step1: parsing webpage and produce stock list
        logprint('Processing stock:', item)
        url_stock_profile = R50_general.general_constants.weblinks['stock_change_record_qq'] % {'stock_id':item}
        soup_profile = gcf.get_webpage(url_stock_profile)
        if soup_profile:
            # func1: fetch name change log
            dfm_item_changes = soup_parse_change_hist(soup_profile)
            if len(dfm_item_changes) > 0:
                gcf.dfm_col_type_conversion(dfm_item_changes,columns={'变动日期':'datetime','公布日期':'datetime'})
                dfm_item_name_changes = dfm_item_changes[dfm_item_changes['变动项目']=='证券简称']
                # print(dfm_item_name_changes)
                dfm_item_name_changes=dfm_item_name_changes.set_index('变动日期')
                dfm_item_name_changes = dfm_item_name_changes.rename(columns={'公布前内容':'原证券简称',
                                                      '公布后内容':'证券简称',
                                                      '公布日期':'简称变动公布日期'})
                del(dfm_item_name_changes['变动项目'])
                dict_cols_cur = {'证券简称': 'nvarchar(50)',
                                 '简称变动公布日期':'datetime',
                                 '原证券简称':'nvarchar(50)'}
                if len(dfm_item_name_changes) > 0:
                    df2db.add_new_chars_and_cols(dict_cols_cur,list(dfm_db_chars['Char_ID']),table_name,dict_misc_pars)
                    # step4: insert transaction data into transaction table
                    market_id = 'SH' if item.startswith('6') else 'SZ'
                    df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(market_id, item, dfm_item_name_changes, table_name,
                                                                        dict_misc_pars,
                                                                        processing_mode='w_update')
                dfm_item_changes['Stock_ID'] = item
                dfm_item_changes['Market_ID'] = 'SH' if item.startswith('6') else 'SZ'
                ls_changes.append(dfm_item_changes)
                # print (dfm_item_name_changes )
            # func2: fetch stock category info
            dfm_item_catgs = soup_parse_catg(soup_profile)
            if type(dfm_item_catgs) != type(None):
                dict_cols_cur_catg = {'所属板块': 'nvarchar(50)'}
                df2db.add_new_chars_and_cols(dict_cols_cur_catg,list(dfm_db_chars_catg['Char_ID']),table_name_concept,
                                             dict_misc_pars_catg)
                market_id = 'SH' if item.startswith('6') else 'SZ'
                for id in range(len(dfm_item_catgs)):
                    if not (dfm_item_catgs.iloc[id]['所属板块'],) in set_db_catg:
                        logprint("Stock %s Concept %s doesn't exist in table ZFCG_Category" %(item,dfm_item_catgs.iloc[id]['所属板块']))
                df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(market_id,item,dfm_item_catgs,table_name_concept,
                                                                   dict_misc_pars_catg,process_mode = 'w_check')
            else:
                logprint("Stock %s can't find category info" %item)


    if ls_changes and mode == 'update_log':
        dfm_changes = pd.concat(ls_changes)
        df2db.load_snapshot_dfm_to_db(dfm_changes,'YY_stock_changes_qq','del&recreate')

def soup_parse_catg(soup):
    # print(soup)
    # 使用正则表达式找到"所属板块"之后的第一个的td节点.请获取这个td节点里面所有标记为a的tag对应的string
    # obselete:注意[^/]*代表<tr>和所属板块之间不能有/这个符号.这样可以防止比配到如下的结果
    # <tr>....</tr><tr>...</tr><tr>...所属板块...</tr>
    str_catg = re.findall('所属板块.*?(<td.*?)</tr>',str(soup),re.I|re.S)
    ls_catgs = []
    if str_catg:
        soup_catg = BeautifulSoup(str_catg[0],'lxml')
#        print(str_catg)
        ls_soup_catg = soup_catg.find_all('a')
        for catg in ls_soup_catg:
            dt_catg = {}
            dt_catg['所属板块'] = catg.string.strip()
            ls_catgs.append(dt_catg)
        return DataFrame(ls_catgs)



def soup_parse_change_hist(soup:BeautifulSoup):
    body_content = soup.find_all('table',class_= 'list')
    # print (body_content[0])
    stock_changes = body_content[1].find_all('tr')
    # print(stock_profile[0].td)
    # print(stock_profile[1].contents)
    stock_changes = stock_changes[2:] if len(stock_changes) > 2 else []
    ls_changes =[]
    for tr in stock_changes:
        change_record = {}
        ls_change_item = list(tr.stripped_strings)
        if len(ls_change_item) == 5:
            change_record['变动项目'] = ls_change_item[0].strip()
            change_record['变动日期'] = ls_change_item[1].strip()
            change_record['公布日期'] = ls_change_item[2].strip()
            change_record['公布前内容'] = ls_change_item[3].strip()
            change_record['公布后内容'] = ls_change_item[4].strip()
            ls_changes.append(change_record)

    return DataFrame(ls_changes)


if __name__ ==  '__main__':
    fetch2DB()