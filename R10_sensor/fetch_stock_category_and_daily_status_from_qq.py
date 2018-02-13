import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re
import time

from datetime import datetime

import R50_general.general_constants
from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

timestamp = datetime.now()
global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

# TODO: current only handle China market category, will parse other market in future
def fetch2DB():


    url_catglist = R50_general.general_constants.weblinks['stock_category_w_detail_qq'][0]

    # t参数:代表不同的category类型
    # 01-腾讯行业
    # 02-概念
    # 03-地域
    # 04-证监会行业

    # 1.1 get category code list
    ls_catg =[]
    for i in range(4):
        url_link = url_catglist %{'catg_type':'0'+str(i+1)}
        # print(url_link)
        soup_category = gcf.get_webpage_with_retry(url_link)
        list_data = re.findall("data:'(.*)'",str(soup_category))
        # TODO: error handling
        assert len(list_data) == 1 , 'No category details can be found under category type %s' %(i+1)

        if i==2 or i==3:
            # need to replace qt with hz, eg. bkqt034600->bkhz034600
            ls_catg.extend([x[:2]+'hz'+x[4:] for x in list_data[0].split(',')])
        else:
            ls_catg.extend(list_data[0].split(','))

    # 1.2 get category detail, such as name etc.
    ls_dfm_catgs = []
    ls_dfm_catgs_trans =[]
    ls_tmp =[]
    for i in range(len(ls_catg)):
        ls_tmp.append(ls_catg[i])
        # every 80 items,get web page in one batch
        # 在for循环中用每n个处理一次时,要注意要用(i+1) % n,因为i是从0开始的.
        if (i+1) % 50 == 0:
            dfm_catgs_tmp, dfm_catgs_trans_tmp = parse_concept(ls_tmp)
            ls_dfm_catgs.append(dfm_catgs_tmp)
            ls_dfm_catgs_trans.append(dfm_catgs_trans_tmp)
            ls_tmp=[]
    if ls_tmp:
        dfm_catgs_tmp, dfm_catgs_trans_tmp = parse_concept(ls_tmp)
        ls_dfm_catgs.append(dfm_catgs_tmp)
        ls_dfm_catgs_trans.append(dfm_catgs_trans_tmp)


    dfm_cur_catg = pd.concat(ls_dfm_catgs)
    dfm_catgs_daily_trans = pd.concat(ls_dfm_catgs_trans)

    # 2.1 insert new category into DB
    # get db category list
    dfm_db_catg = df2db.get_catg('QQ')
    dfm_new_catg = gcf.dfm_A_minus_B(dfm_cur_catg,dfm_db_catg,['Catg_Origin','Catg_Type','Catg_Name'])

    # print(dfm_db_catg,dfm_cur_catg,dfm_new_catg,sep = '\n')
    if len(dfm_new_catg) > 0:
        # gcf.dfmprint(dfm_cur_catg)
        logprint('New Category added:\n' ,'\n'.join([dfm_new_catg.iloc[i]['Catg_Type'] + ':' +
                                                   dfm_new_catg.iloc[i]['Catg_Name']
                                                   for i in range(len(dfm_new_catg))]), sep = '\n')
        dfm_new_catg['Catg_Id'] = dfm_new_catg['Catg_Name']
        df2db.load_snapshot_dfm_to_db(dfm_new_catg,'ZCFG_category',w_timestamp=True)

    # inform obsolete category to user to make sure no error occures,no action in db side
    dfm_obselete_catg = gcf.dfm_A_minus_B(dfm_db_catg, dfm_cur_catg,  ['Catg_Origin', 'Catg_Type', 'Catg_Name'])
    if len(dfm_obselete_catg) > 0:
        # print(dfm_obselete_catg)
        for index,row in dfm_obselete_catg.iterrows():
            logprint('Category Type %s Name %s is obselete! Please double check!' %(row['Catg_Type'],row['Catg_Name']))

    # 2.2 insert new stock category relationship into DB
    # create DB table and chars
    table_name_concept = R50_general.general_constants.dbtables['stock_category_relation_qq']
    df2db.create_table_by_template(table_name_concept,table_type='stock_date_multi_value')
    # get chars for stock category
    dfm_db_chars_catg = df2db.get_chars('QQ', ['CATG'])
    dict_misc_pars_catg = {}
    dict_misc_pars_catg['char_origin'] = 'QQ'
    dict_misc_pars_catg['char_freq'] = "D"
    dict_misc_pars_catg['allow_multiple'] ='Y'
    dict_misc_pars_catg['created_by'] = dict_misc_pars_catg['update_by'] = global_module_name
    dict_misc_pars_catg['char_usage'] = 'CATG'
    dict_cols_cur_catg = {'Catg_Type': 'nvarchar(50)','Catg_Name':'nvarchar(50)'}
    df2db.add_new_chars_and_cols(dict_cols_cur_catg, list(dfm_db_chars_catg['Char_ID']), table_name_concept,
                                 dict_misc_pars_catg)


    #func2: fetch stock category info
    dt_stk_catgs = parse_stock_under_catg(dfm_cur_catg)
    # print(dt_stk_catgs)

    #get stock list:
    dfm_cn_stocks = df2db.get_cn_stocklist()
    dfm_cn_stocks['marstk_id'] = dfm_cn_stocks['Market_ID'] + dfm_cn_stocks['Stock_ID']

    for index,row in dfm_cn_stocks.iterrows():
        ls_catgs = dt_stk_catgs.get(row['marstk_id'],None)
        if ls_catgs:
            dfm_item_catgs = DataFrame(ls_catgs)
            df2db.load_dfm_to_db_multi_value_by_mkt_stk_cur(row['Market_ID'],
                                                            row['Stock_ID'],
                                                            dfm_item_catgs,
                                                            table_name_concept,
                                                            dict_misc_pars_catg,
                                                            process_mode = 'w_check')
        else:
            logprint("Stock %s doesn't have category assigned" %(row['Stock_ID']+':'+ row['Stock_Name']))

    # 2.3 insert category daily detail into DB
    # create DB table and chars
    table_name_catg_trans = R50_general.general_constants.dbtables['category_daily_trans_qq']
    df2db.create_table_by_template(table_name_catg_trans,table_type='catg_date')
    # get chars for stock category trans
    dfm_db_chars_catgtrans = df2db.get_chars('QQ', ['CATG_TRANS'])
    dict_misc_pars_catgtrans = {}
    dict_misc_pars_catgtrans['char_origin'] = 'QQ'
    dict_misc_pars_catgtrans['char_freq'] = "D"
    dict_misc_pars_catgtrans['allow_multiple'] ='N'
    dict_misc_pars_catgtrans['created_by'] = dict_misc_pars_catgtrans['update_by'] = global_module_name
    dict_misc_pars_catgtrans['char_usage'] = 'CATG_TRANS'
    dict_cols_cur_catgtrans = {'上涨家数': 'int',
                               '平盘家数':'int',
                               '下跌家数':'int',
                               '总家数':'int',
                               '平均价格':'decimal(14, 4)',
                               '涨跌额':'decimal(14, 4)',
                               '涨跌幅':'decimal(8,6)',
                               '总成交手数': 'decimal(18, 2)',
                               '总成交额万元':'decimal(20,2)',
                                 }

    df2db.add_new_chars_and_cols(dict_cols_cur_catgtrans, list(dfm_db_chars_catgtrans['Char_ID']), table_name_catg_trans,
                                 dict_misc_pars_catgtrans)
    # insert into db
    df2db.load_dfm_to_db_cur(dfm_catgs_daily_trans,['Catg_Type','Catg_Name','Trans_Datetime'],table_name_catg_trans,
                             dict_misc_pars_catgtrans, process_mode='w_update')

def parse_stock_under_catg(dfm_catgs:DataFrame) ->dict:
    """
    return a dictionary, key:value -> market code + stock code: list of category related to it
    :param dfm_catgs:
    :return:
    """
    dt_stkcatgs = {}
    for index,row in dfm_catgs.iterrows():
        catg_code = row['Catg_Reference']
        retry_times = 1
        while True:
            retry_times +=1
            url_catgstklist = R50_general.general_constants.weblinks['stock_category_w_detail_qq'][2] % {'catg_code':catg_code}
            soup_stklst = gcf.get_webpage_with_retry(url_catgstklist)
            list_data = re.findall("data:'(.*)'", str(soup_stklst))

            if list_data:
                ls_stk = list_data[0].split(',')
                ls_stk = [stk.strip().upper() for stk in ls_stk]
                # print(ls_stk)
                for stkcode in ls_stk:
                    ls_catgs = dt_stkcatgs.get(stkcode, [])
                    # print(ls_catgs)
                    if ls_catgs:
                        ls_catgs.append({'Catg_Type': row['Catg_Type'],'Catg_Name': row['Catg_Name']})
                    else:
                        dt_stkcatgs[stkcode] = [{'Catg_Type': row['Catg_Type'],'Catg_Name': row['Catg_Name']}]
                break
            elif retry_times < 10:
                time.sleep(3)
            else:
                # TODO: error handling
                logprint('Exception: Catg %s has no stock assigned' %row['Catg_Name'] )
                assert 0==1, 'inconsistent found, please check!'

    return dt_stkcatgs


def parse_concept(ls_catgs:list):
    catgs_num = len(ls_catgs)
    str_catgs =','.join(ls_catgs)
    url_catgdetail = R50_general.general_constants.weblinks['stock_category_w_detail_qq'][1] % {'catg_list': str_catgs}
    #print(url_catgdetail)
    soup_catg = gcf.get_webpage_with_retry(url_catgdetail)
    # print(soup_catg)

    dt_type = {'01':'腾讯行业',
               '02':'概念',
               '03':'地域',
               '04':'证监会行业'}

    ls_catg_detail = re.findall('="(.*)";',str(soup_catg))

    ls_dfm_catg = []
    ls_dfm_catg_daily_trans =[]

    for item in ls_catg_detail:
        ls_item = item.split('~')
        if len(ls_item) > 10:
            dt_line = {}
            dt_line['Catg_Origin'] = 'QQ'
            dt_line['Catg_Type'] = dt_type[ls_item[0][:2]]
            dt_line['Catg_Name'] = ls_item[1].strip()
            dt_line['Catg_Reference'] = ls_item[0].strip()
            dt_line['Created_datetime'] = timestamp
            dt_line['Created_by'] = global_module_name
            ls_dfm_catg.append(dt_line)

            dt_catg_daily_trans ={}
            # dt_catg_daily_trans['Catg_Origin']  =  'QQ'
            dt_catg_daily_trans['Catg_Type']    = dt_type[ls_item[0][:2]]
            dt_catg_daily_trans['Catg_Name']    = ls_item[1].strip()
            dt_catg_daily_trans['上涨家数']      = int(ls_item[2])
            dt_catg_daily_trans['平盘家数']      = int(ls_item[3])
            dt_catg_daily_trans['下跌家数']      = int(ls_item[4])
            dt_catg_daily_trans['总家数']        = int(ls_item[5])
            dt_catg_daily_trans['平均价格']      = float(ls_item[6])
            dt_catg_daily_trans['涨跌额']        = float(ls_item[7])
            dt_catg_daily_trans['涨跌幅']        = float(ls_item[8])/100
            dt_catg_daily_trans['总成交手数']    = float(ls_item[9])
            dt_catg_daily_trans['总成交额万元']  = float(ls_item[10])
            ls_dfm_catg_daily_trans.append(dt_catg_daily_trans)
        else:
            # TODO: error handling
            assert 0==1,'TODO:error handling!'

    # TODO:error handling
    if len(ls_dfm_catg) != catgs_num:
        # not all catg code get detail line item, it is an exception
        set_delta_catg = set([x[-6:] for x in ls_catgs]) - set([y['Catg_Reference'] for y in ls_dfm_catg ])
        logprint("Below Category reference can't get daily details:" )
        logprint(set_delta_catg)
        assert 0==1, 'inconsistent found, please check manually'

    return (DataFrame(ls_dfm_catg),DataFrame(ls_dfm_catg_daily_trans))

if __name__ == '__main__':
    fetch2DB()
    # soup_tmp = gcf.get_webpage('http://qt.gtimg.cn/q=bkqt012067')
    #
    # print(soup_tmp.prettify())