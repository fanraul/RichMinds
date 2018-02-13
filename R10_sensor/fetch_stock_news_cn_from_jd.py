import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import re
import sys
import io

from datetime import datetime

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
# from pyodbc import IntegrityError
from sqlalchemy.exc import IntegrityError
global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

general_pages_to_fetch = 200
general_start_page = 0
general_pages_to_split = 1

'''
Program purpose: get news list and download html
京东的网页id不是唯一标识, 同样的id可能对应不同的文章
故使用文章的时间和文章的title作为唯一的key值,但是京东的文章时间有时也会变,所以时间取到小时,即title和精确到小时的时间确定是否为
唯一的文章.
计划每天抓取,检查最近的100页
'''

def fetch2DB():
    # create jd news table
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'JD'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'Y'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'NEWS'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['newslist_jd']
    df2db.create_table_by_template(table_name,table_type='jd_newslist')

    # get processed new list from DB, downloaded or useless, no need to download html or update db for these entries
    key_cols = ['Region_ID','News_datetime','Title']
    dfm_newslist_processed = get_newlist_processed()[key_cols]

    # update every general_pages_to_split pages
    ls_dfmnews = []
    webscrap_times = general_pages_to_fetch // general_pages_to_split + 1
    try:
        for i in range(webscrap_times):
            start_page_cur = i * general_pages_to_split + general_start_page
            if i == webscrap_times -1:
                pages_to_fetch_cur = general_pages_to_fetch % general_pages_to_split +1
            else:
                pages_to_fetch_cur = general_pages_to_split

            dfm_newslist = parse_newslist(pages_to_fetch_cur,start_page_cur)
            dfm_newslist.drop_duplicates(subset = ['News_datetime','Title'],inplace=True)
            dfm_newslist['Region_ID'] = 'CN'
            dfm_newslist_toprocess = gcf.dfm_A_minus_B(dfm_newslist, dfm_newslist_processed, key_cols)
            if len(dfm_newslist_toprocess) > 0:
                dfm_newslist_toprocess['News_FileID'] = dfm_newslist_toprocess.apply(fetch2FILE,axis=1)
                dfm_newslist_toprocess['News_downloaded'] = dfm_newslist_toprocess.apply(lambda s: 'X' if not pd.isnull(s['News_FileID']) else 'E',                                                                                         axis =1)
                # gcf.dfmprint(dfm_newslist_toprocess)
                try:
                    df2db.dfm_to_db_insert_or_update(dfm_newslist_toprocess, key_cols, table_name, global_module_name, process_mode='w_update')
                except IntegrityError as e:
                    # 如果错误是主键重复,则忽略(应该是全角和半角的问题)
                    if e.orig.args[0] == '23000':
                        logprint('DB primary key duplicated',e.orig,e.params,add_log_files='I')
                    else:
                        raise e
                ls_dfmnews.append(dfm_newslist_toprocess)
                dfm_newslist_processed = dfm_newslist_processed.append(dfm_newslist_toprocess[key_cols],ignore_index=True)
    # except IntegrityError:
    finally:
        if len(ls_dfmnews) >0 :
            pd.concat(ls_dfmnews).to_excel('newslist.xls')

def parse_newslist(pages,start_page)-> DataFrame  :
    logprint('Get JD stock news from pages %s to pages %s:' %(start_page,start_page+pages-1))
    ls_dfmnewslist = []
    for i in range(pages):
        url_newslist_page = R50_general.general_constants.weblinks['stock_newslist_jd'] %(i+start_page)
        url_prefix_newsdetail = R50_general.general_constants.weblinks['jd_stock_news_details_prefix']
        soup_newslist = gcf.get_webpage_with_retry(url_newslist_page)
        if soup_newslist:
            body_newslist = soup_newslist.find_all('li',class_ = "clearfix")
            for newslink in body_newslist:
                str_news_link = newslink.div.a.get('href').strip()
                dt_newslink ={}
                # some title has a special unicode '\u200b
                dt_newslink['Title'] = newslink.div.a.string.strip().replace(u'\u200b','')
                dt_newslink['Weblink'] = url_prefix_newsdetail + str_news_link
                news_datetime = '20' + newslink.find_all('span', "date-time")[0].string.strip()
                dt_newslink['News_actual_datetime'] = datetime.strptime(news_datetime , '%Y-%m-%d %H:%M')
                dt_newslink['News_datetime'] = datetime.strptime(news_datetime[:-3],'%Y-%m-%d %H')
                dt_newslink['Page_num_last_fetch'] = i+start_page
                # dt_newslink['News_ID'] = re.findall('detail-([0-9]+).html',str_news_link)[0]
                ls_dfmnewslist.append(dt_newslink)
    return DataFrame(ls_dfmnewslist)

    # for index,row in dfm_newslist.iterrows():


'''
    <li class="clearfix">
        <div class="c-no-le ">
            <a href="/info/detail-106208.html" clstag="jr|keycount|gupiao_zixun|rmzx_uszx1"  class="c-li-title" target="_blank">涨停板预测:今日22股有望涨停！ </a>
            <p class="c-ke-text"></p>
            <p class="c-date-com">
                <span class="date-time">&nbsp;17-12-01 00:19</span>
                <span class="come">来源: 京东股票</span>
                <span class="comment">1条评论</span>
            </p>
        </div>
    </li>
'''

def get_newlist_processed():
    table_name = R50_general.general_constants.dbtables['newslist_jd']
    dfm_cond = DataFrame([{'db_col': 'News_downloaded', 'db_oper': '=', 'db_val': "'X'"},
                          {'db_col': 'Ind_useless', 'db_oper': '=', 'db_val': "'X'"}, ])

    return df2db.get_data_from_DB(table_name, dfm_cond,'OR')


def fetch2FILE(row:Series):
    # timestamp as file id , check topics in the html, if topics same as title, return X, else return E
    '''
    :param row:
    :return:
    '''
    url_news = row['Weblink']
    html_news_item = gcf.get_webpage_with_retry(url_news,flg_return_rawhtml=True,time_wait=5)
    # print(row['Title'])
    if html_news_item:
        soup_news_item=BeautifulSoup(html_news_item,"lxml")
        titles = soup_news_item.find_all('title')
        if not titles:
            logprint('News %s weblink %s content incorrect, can not find title in html' %(row['Title'],row['Weblink']))
            return
        for tag in titles:
            # some title has a special unicode '\u200b
            title = tag.text.strip()[:-10].strip().replace(u'\u200b','')
            # check html title is the same as weblink title, if not the same, return 'E'
            if title != row['Title']:
                logprint(
                    'News %s weblink %s html content inconsistent, title in html is %s' %(row['Title'],  row['Weblink'],
                                                                                          title),add_log_files='I')
                return
        if len(soup_news_item.find_all('article')) > 0:
            # there is article session in the html
            timestamp = datetime.now()
            news_fileid = str(timestamp)[2:].replace('-','').replace(':','').replace(' ','') + '.html'
            filename_news = R50_general.general_constants.Global_path_news_details_jd + news_fileid
            file_news = open(filename_news,'wb')
            file_news.write(html_news_item)
            file_news.close()
            logprint('News %s is downloaded' % row['Title'])
            return news_fileid
        else:
            logprint('News %s weblink %s does not have article session, no content!' %(row['Title'],row['Weblink']))
            return
    logprint('News %s weblink %s html can not access' %(row['Title'],row['Weblink']))
    return


if __name__ == '__main__':
    fetch2DB()