import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re

from datetime import datetime

import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint


#TODO: 1.add error handling and log output
#TODO: 2.目前只处理A股数据,以后是否要增加处理B股或其他基金,国债回购编码
#TODO: 3.如何获得当前的程序名称作为创建人或修改者

def fetch2DB():
    #init step
    timestamp = datetime.now()

    #Step1: parsing webpage and produce stock list
    url_eastmoney_stock_list = R50_general.general_constants.weblinks['stock_list_easymoney']
    req = urllib.request.Request(url_eastmoney_stock_list)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'
    req.add_header('User-Agent', user_agent)
    req.add_header('Referer',url_eastmoney_stock_list)
    try:
        response = urllib.request.urlopen(req)
        html = response.read()
    #print (html.decode("gb2312"))
        soup = BeautifulSoup(html,"lxml")
    #        print (soup.prettify())
    except urllib.error.HTTPError as e:
        print(e.code)
        print(e.read().decode("utf8"))

    body_content = soup.find_all(class_= 'quotebody')
    #print (type(body_content[0]))
    stock_content = body_content[0].find_all('li')
    # print ("Type:",type(stock_content[0]))
    # print("Content:",stock_content[0])
    # print("Name:",stock_content[0].name)
    # print("Subtag:",stock_content[0].a)
    # print("String:",stock_content[0].string)

    seq_no = 0
    ls_stock = []
    for stock_line in stock_content:
        stock_temp_info = stock_line.string
        stock_info = stock_temp_info.split('(')
        if stock_info:
            stock_id = stock_info[1][:-1].strip()
            stock_name = stock_info[0].strip()
            ls_stock.append((timestamp,seq_no,stock_id,stock_name))
        seq_no+=1


    #step2: get DB connection
    dcm_sql = dcm()
    engine = dcm_sql.getengine()
    conn = dcm_sql.getconn()


    #step3: insert to log table YY_Stocklist_eastmoney
    ins_str = "INSERT INTO YY_Stocklist_eastmoney (Time_Id,Seq_No,Stock_ID,Stock_Name) VALUES (?,?,?,?)"
    conn.execute(ins_str,tuple(ls_stock))


    #step4: update new stock id and modify stock_name if required in table stock_basic_info
    #get current stock list
    dfm_stocks = pd.read_sql_query('select [Market_ID], [Stock_ID],[Stock_Name] from stock_basic_info'
                                   , engine)

    dfm_stocks['Market_Stock_ID'] = dfm_stocks['Market_ID'] + dfm_stocks['Stock_ID']
    dt_stocks_existed = dict(zip(dfm_stocks['Market_Stock_ID'], dfm_stocks['Stock_Name']))

    # {'COMEXCOMEX白银': 'COMEX白银',
    #  'COMEXCOMEX黄金': 'COMEX黄金',
    #  'NYMEXNYMEX原油': 'NYMEX原油',
    #  'USMSSQL测试1': 'MSSQL测试2',
    #  'USMSSQL测试2': 'MSSQL测试3'}

    #add new stock id
    ls_insert_stocks = []
    #update stock name if required
    ls_update_stocks = []

    for stock_item in ls_stock:
        stock_id = stock_item[2]
        stock_name = stock_item[3]
        if stock_id.startswith('0') or stock_id.startswith('3'):
            market_id = 'SZ'
        elif stock_id.startswith('6'):
            market_id = 'SH'
        else:
            continue

        if (market_id + stock_id) not in dt_stocks_existed.keys():
            logprint('new stock id added', stock_id,stock_name)
            ls_insert_stocks.append((market_id, stock_id, stock_name, timestamp, 'fetch_stocklist_from_eastmoney'))
        elif stock_name != dt_stocks_existed[market_id + stock_id]:
            logprint("stock id %s's name changed from %s to %s" %(stock_id,
                                                                  dt_stocks_existed[market_id + stock_id],stock_name))
            ls_update_stocks.append((stock_name, timestamp, 'fetch_stocklist_from_eastmoney', market_id, stock_id))
    if ls_insert_stocks:
        ins_str = "INSERT INTO stock_basic_info (Market_ID,Stock_ID,Stock_Name,Created_datetime,Created_by) VALUES (?,?,?,?,?)"
        conn.execute(ins_str, tuple(ls_insert_stocks))

    if ls_update_stocks:
        update_str = '''UPDATE stock_basic_info SET Stock_Name = ?,Last_name_modified_datetime = ?,Last_name_modified_by = ? 
                        WHERE Market_ID = ? AND Stock_ID = ? '''
        conn.execute(update_str, tuple(ls_update_stocks))

    #laststep:
    dcm_sql.closeconn()

if __name__ ==  '__main__':
    fetch2DB()