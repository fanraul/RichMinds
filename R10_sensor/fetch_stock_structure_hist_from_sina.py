import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import re
from datetime import datetime
import gc

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

timestamp = datetime.now()

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)


def fetch2DB(stockid:str = ''):

    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('SINA', ['STRC'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'SINA'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'STRC'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_structure_sina']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {'变动日期':'datetime',
                     '公告日期':'datetime',
                     '变动原因':'nvarchar(50)',
                     '总股本': 'decimal(18,4)',
                     # '流通股': [],
                     '流通A股': 'decimal(18,4)',
                     '高管股': 'decimal(18,4)',
                     '限售A股': 'decimal(18,4)',
                     '流通B股': 'decimal(18,4)',
                     '限售B股': 'decimal(18,4)',
                     '流通H股': 'decimal(18,4)',
                     '国家股': 'decimal(18,4)',
                     '国有法人股': 'decimal(18,4)',
                     '境内法人股': 'decimal(18,4)',
                     '境内发起人股': 'decimal(18,4)',
                     '募集法人股': 'decimal(18,4)',
                     '一般法人股': 'decimal(18,4)',
                     '战略投资者持股': 'decimal(18,4)',
                     '基金持股': 'decimal(18,4)',
                     '转配股': 'decimal(18,4)',
                     '内部职工股': 'decimal(18,4)',
                     '优先股': 'decimal(18,4)',
                     '股本单位':'nvarchar(50)',
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)


    # step2.1: get current stock list
    dfm_stocks = df2db.get_cn_stocklist(stockid)
    # print(dfm_stocks)
    for index,row in dfm_stocks.iterrows():
        logprint('Processing stock %s' %row['Stock_ID'])
        # step1:parse webpage and get raw data
        url_stock_structure = R50_general.general_constants.weblinks['stock_structure_sina'] % row['Stock_ID']
        # print(url_link)
        soup_stock_structure = gcf.get_webpage(url_stock_structure)
        dfm_stk_strc = soup_parse_stock_structure(soup_stock_structure)
        # TODO: error handling
        assert len(dfm_stk_strc) > 0 , 'No stock structure details can be found for stockid %s' %row['Stock_ID']

        # step2: format raw data into prop data type
        dfm_stk_strc['股本单位'] = '万股'
        gcf.dfmprint(dfm_stk_strc)

        # formating
        dfm_fmt_stk_strc = format_dfm_stk_strc(dfm_stk_strc)
        # only one entry allowed in one day, so need to combine multiple changes in one day into one entry
        process_duplicated_entries(dfm_fmt_stk_strc,row['Stock_ID'])
        gcf.dfmprint(dfm_fmt_stk_strc)
        df2db.load_dfm_to_db_single_value_by_mkt_stk_w_hist(row['Market_ID'], row['Stock_ID'], dfm_fmt_stk_strc, table_name,
                                                            dict_misc_pars,
                                                            processing_mode='w_update')
        gc.collect()
        # time.sleep(10)

def process_duplicated_entries(dfm_stk_strc:DataFrame,stockid):
    dfm_duplicated = dfm_stk_strc[dfm_stk_strc.duplicated(['变动日期'])]
    # print(dfm_duplicated)
    dfm_stk_strc.drop_duplicates('变动日期',inplace=True)
    for index, row in dfm_duplicated.iterrows():
        # dfm_stk_strc.loc[index]['变动原因'] = dfm_stk_strc.loc[index]['变动原因'] +'|'+row['变动原因']
        dfm_stk_strc.loc[index,'变动原因'] = dfm_stk_strc.loc[index]['变动原因'] + '|' + row['变动原因']
        logprint('Stock %s 变动日期 %s 记录合并到主记录中. %s' %(stockid,row['变动日期'],tuple(row)))

def format_dfm_stk_strc(dfm_strc):
    cols_for_conv ={'变动日期':'datetime',
                     '公告日期':'datetime',
                    '变动原因': 'varchar',
                     '总股本': 'float',
                     # '流通股': [],
                     '流通A股': 'float',
                     '高管股': 'float',
                     '限售A股': 'float',
                     '流通B股': 'float',
                     '限售B股': 'float',
                     '流通H股': 'float',
                     '国家股': 'float',
                     '国有法人股': 'float',
                     '境内法人股': 'float',
                     '境内发起人股': 'float',
                     '募集法人股': 'float',
                     '一般法人股': 'float',
                     '战略投资者持股': 'float',
                     '基金持股': 'float',
                     '转配股': 'float',
                     '内部职工股': 'float',
                     '优先股': 'float',
                    }

    gcf.dfm_col_type_conversion(dfm_strc, columns=cols_for_conv)
    # dfm_item_name_changes = dfm_item_changes[dfm_item_changes['变动项目'] == '证券简称']
    # print(dfm_item_name_changes)
    dfm_strc['colforindex'] = dfm_strc['变动日期']
    return dfm_strc.set_index('colforindex')
    # dfm_item_name_changes = dfm_item_name_changes.rename(columns={'公布前内容': '原证券简称',
    #                                                               '公布后内容': '证券简称',
    #                                                               '公布日期': '简称变动公布日期'})
    # del (dfm_item_name_changes['变动项目'])


def soup_parse_stock_structure(soup):
    strc_tables = soup.find_all('table',id= re.compile('StockStructureNewTable\d*'))
    # print (strc_tables[0])

    # initial a dictionary for parsing result
    dt_strc_items = {'变动日期':[],
                     '公告日期':[],
                     '变动原因':[],
                     '总股本': [],
                     # '流通股': [],
                     '流通A股': [],
                     '高管股': [],
                     '限售A股': [],
                     '流通B股': [],
                     '限售B股': [],
                     '流通H股': [],
                     '国家股': [],
                     '国有法人股': [],
                     '境内法人股': [],
                     '境内发起人股': [],
                     '募集法人股': [],
                     '一般法人股': [],
                     '战略投资者持股': [],
                     '基金持股': [],
                     '转配股': [],
                     '内部职工股': [],
                     '优先股': [],
                     }

    for str_table in strc_tables:
        # tr_contents = str_table.tbody.find_all('tr')
        # gcf.print_list_nice(tr_contents)
        for key in dt_strc_items:
            dt_strc_items[key].extend(get_subsequent_tds_by_first_td_content(str_table,key))
    dfm_stk_strc = DataFrame(dt_strc_items)
    return dfm_stk_strc
    # print(stock_profile[0].td)
    # print(stock_profile[1].contents)

    # stock_changes = stock_changes[2:] if len(stock_changes) > 2 else []
    # ls_changes =[]
    # for tr in stock_changes:
    #     change_record = {}
    #     ls_change_item = list(tr.stripped_strings)
    #     if len(ls_change_item) == 5:
    #         change_record['变动项目'] = ls_change_item[0].strip()
    #         change_record['变动日期'] = ls_change_item[1].strip()
    #         change_record['公布日期'] = ls_change_item[2].strip()
    #         change_record['公布前内容'] = ls_change_item[3].strip()
    #         change_record['公布后内容'] = ls_change_item[4].strip()
    #         ls_changes.append(change_record)
    #
    # return DataFrame(ls_stk_strc)

def get_subsequent_tds_by_first_td_content(soup,key_tdnode:str)->list:
    """
    使用正则表达式找到"key_tdnode"之后的"/tr"之前的所有td节点.获取这些td节点对应的string
    :param soup:
    :param str_first_tdnode:
    :return:
    """
    # print(soup)
    # print(key_tdnode)
    # 通过[^']排除那些name = '流通A股'的节点,key_tdnode这个字符后面不能是单引号.
    str_tds = re.findall("%s[^'].*?(<td.*?)</tr>" %key_tdnode,str(soup),re.I|re.S)
    # print_list_nice(str_tds)
    soup_tds = BeautifulSoup(str_tds[0], 'lxml')
    tags_td = soup_tds.find_all(name = 'td')
    ls_str_td = []
    for tag_td in tags_td:
        # print(tag_td)
        ls_str_td.append(tag_td.string.strip() if tag_td.string else '')
    return ls_str_td[:]

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier=global_module_name, func_to_call= fetch2DB, wait_seconds= 600)

if __name__ == '__main__':
    #fetch2DB('600061')
    # fetch2DB()
    auto_reprocess()