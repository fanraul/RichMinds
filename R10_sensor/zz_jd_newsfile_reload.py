import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import re

from datetime import datetime

import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db

import os

folder_name = r'C:\80_Business_docs\news\jd'

ls_dfm_newslist = []

for filename in os.listdir(folder_name):
    fileurl = folder_name + r'\\' + filename
    # print(fileurl)
    htmlfile = open(fileurl, 'r',encoding = 'UTF-8')  #以只读的方式打开本地html文件
    htmlpage = htmlfile.read()
    #print htmlpage
    soup = BeautifulSoup(htmlpage, "lxml")  #实例化一个BeautifulSoup对象
    titles = soup.find_all('title')
    for tag in titles:
        title = tag.text.strip()[:-10].strip()
        print(title)
        dt_news ={}
        dt_news['title'] = title
        dt_news['filename'] = filename
        news_datetimes = soup.find_all(class_ = 'typical details-typical font-gray')
        if len(news_datetimes) > 0:
            dt_news['news_datetime'] = ' '.join(news_datetimes[0].text.split(' ')[1:])[:19]
        ls_dfm_newslist.append(dt_news)




dfm_newslist = DataFrame(ls_dfm_newslist)
dfm_newslist.to_excel('newsfile.xls')