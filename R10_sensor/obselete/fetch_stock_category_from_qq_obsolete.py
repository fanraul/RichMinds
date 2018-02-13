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
# TODO: current only handle China market category, will parse other market in future
def fetch2DB():
    url_category = R50_general.general_constants.weblinks['stock_category_qq']
    soup_category = gcf.get_webpage(url_category)
    # parse data like below:
    # <a class ="clk-mo-li" href="?id=bd021225" id="a-l-bd021225" title="广东自贸区" >  广东自贸区 </a>
    # bd01代表腾讯行业
    # Bd02代表概念
    # Bd03代表地域
    # Bd04代表证监会行业

    dt_type = {'bd01':'腾讯行业',
               'bd02':'概念',
               'bd03':'地域',
               'bd04':'证监会行业'}
    ls_category = []
    for type in dt_type:
        ls_category.extend(parse_concept(soup_category,type))
#    gcf.print_list_nice(ls_category)
    ls_dfm_catg = []
    timestamp = datetime.now()
    for (type,catg) in ls_category:
        dt_line = {}
        dt_line['Catg_Origin'] = 'QQ'
        dt_line['Catg_Type'] = dt_type[type]
        dt_line['Catg_Name'] = catg
        dt_line['Created_datetime'] = timestamp
        dt_line['Created_by'] = 'fetch_stock_category_from_qq'
        ls_dfm_catg.append(dt_line)
        logprint('New Category %s:%s added.' %(dt_type[type],catg))
    dfm_cur_catg = DataFrame(ls_dfm_catg)
    # get db category list
    dfm_db_catg = df2db.get_catg('QQ')
    dfm_new_catg = gcf.dfm_A_minus_B(dfm_cur_catg,dfm_db_catg,['Catg_Origin','Catg_Type','Catg_Name'])
    # print(dfm_cur_catg ,dfm_db_catg ,dfm_new_catg,sep='\n')
    df2db.load_snapshot_dfm_to_db(dfm_new_catg,'ZCFG_category',w_timestamp=True)


def parse_concept(tmp_soup,cate_type:str):
    # cate_industry       = re.findall('href="\?id=bd01(.*?)">(.*?)<',str(soup_category))
    # cate_concept        = re.findall('href="\?id=bd02(.*?)">(.*?)<',str(soup_category))
    # cate_region         = re.findall('href="\?id=bd03(.*?)">(.*?)<',str(soup_category))
    # cate_zjh_industry   = re.findall('href="\?id=bd04(.*?)">(.*?)<',str(soup_category))
    ls_category = []
    ls_tmp_category = re.findall('href="\?id=%s(.*?)>(.*?)<' %cate_type,str(tmp_soup))
    tmp_category = ''
    for item in ls_tmp_category:
        if len(item) != 2:
            raise
        tmp_category = item[1]
        tmp_title = re.findall('title="(.*?)"',item[0])
        if len(tmp_title) == 1:
            tmp_category = tmp_title[0]
        ls_category.append((cate_type,tmp_category))
    return ls_category[:]



if __name__ == '__main__':
    fetch2DB()