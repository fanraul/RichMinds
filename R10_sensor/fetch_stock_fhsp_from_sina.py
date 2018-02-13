
# TODO: This program is in-process, no finished,can't use!!

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

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

def fetch2DB(stockid:str = ''):

    # # init step
    # # create DD tables for data store and add chars for stock structure.
    # # get chars for name change hist
    # dfm_db_chars = df2db.get_chars('SINA', ['FHSP'])
    # dict_misc_pars = {}
    # dict_misc_pars['char_origin'] = 'SINA'
    # dict_misc_pars['char_freq'] = "D"
    # dict_misc_pars['allow_multiple'] = 'N'
    # dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = 'fetch_stock_structure_hist_from_sina'
    # dict_misc_pars['char_usage'] = 'STRC'
    #
    # # check whether db table is created.
    # table_name = R50_general.general_constants.dbtables['stock_structure_sina']
    # df2db.create_table_by_template(table_name,table_type='stock_date')
    # dict_cols_cur = {'变动日期':'datetime',
    #                  '公告日期':'datetime',
    #                  '变动原因':'nvarchar(50)',
    #                  '总股本': 'decimal(18,4)',
    #                  # '流通股': [],
    #                  '流通A股': 'decimal(18,4)',
    #                  '高管股': 'decimal(18,4)',
    #                  '限售A股': 'decimal(18,4)',
    #                  '流通B股': 'decimal(18,4)',
    #                  '限售B股': 'decimal(18,4)',
    #                  '流通H股': 'decimal(18,4)',
    #                  '国家股': 'decimal(18,4)',
    #                  '国有法人股': 'decimal(18,4)',
    #                  '境内法人股': 'decimal(18,4)',
    #                  '境内发起人股': 'decimal(18,4)',
    #                  '募集法人股': 'decimal(18,4)',
    #                  '一般法人股': 'decimal(18,4)',
    #                  '战略投资者持股': 'decimal(18,4)',
    #                  '基金持股': 'decimal(18,4)',
    #                  '转配股': 'decimal(18,4)',
    #                  '内部职工股': 'decimal(18,4)',
    #                  '优先股': 'decimal(18,4)',
    #                  '股本单位':'nvarchar(50)',
    #                 }
    # df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)





    stockurl ='http://stocks.sina.cn/sh/finance?vt=4&code=sh600536'

    # req = urllib.request.Request(stockurl)
    #
    # user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'
    # req.add_header('User-Agent', user_agent)
    # req.add_header('Referer','http://stocks.sina.cn/sh/finance?vt=4&code=sz300037')


    # try:
    #     response = urllib.request.urlopen(req)
    #     html = response.read()
    # #print (html.decode("gb2312"))
    #     soup = BeautifulSoup(html,"lxml")
    # #    print (soup.prettify())
    #
    # except urllib.error.HTTPError as e:
    #     print(e.code)
    #     print(e.read().decode("utf8"))

    soup = gcf.get_webpage(stockurl)

    body_content = soup.find_all(class_= 'content')
    if len(body_content) > 0:
        lines = body_content[0].get_text().split('\n')
    ls_fhsp = []
    dt_fhspitem ={}
    for line in lines:
        line = line.strip()
        if line.startswith('公告日期'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['公告日期']=words[1]
        if line.startswith('送股(股)/10股'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['送股(股)/10股']=words[1]
        if line.startswith('转增(股)/10股'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['转增(股)/10股']=words[1]
        if line.startswith('派息(税前)(元)/10股'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['派息(税前)(元)/10股']=words[1]
        if line.startswith('进度'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['进度']=words[1]
        if line.startswith('除权除息日'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['除权除息日']=words[1]
        if line.startswith('股权登记日'):
            words = line.split(':')
            if len(words) > 1: dt_fhspitem['股权登记日']=words[1]
            ls_fhsp.append(dt_fhspitem.copy())
            dt_fhspitem ={}

    for item in ls_fhsp:
        print (item)

def auto_reprocess():
    ahf.auto_reprocess_dueto_ipblock(identifier='fetch_stock_fhsp_from_sina', func_to_call= fetch2DB, wait_seconds= 600)

if __name__ == '__main__':
    fetch2DB('300038')
    # auto_reprocess()