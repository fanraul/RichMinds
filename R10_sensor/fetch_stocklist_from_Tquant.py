import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from bs4 import BeautifulSoup
import urllib.request
import re
import time

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import R90_tquant.getdata as gt
import R90_tquant.myquant as mt
import R50_general.dfm_to_table_common as df2db
import R50_general.general_helper_funcs as gcf

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

def fetch2DB():
    """
    get_shse():上交所的股票列表，包含B股。
    get_szse():深交所的股票列表，包含B股。
    get_dce():大连期货交易所的上市品种列表
    get_czce():郑州期货交易所的上市品种列表
    get_shfe():上海期货交易所的上市品种列表
    get_cffex():中国金融期货交易所的上市品种列表
    get_fund():沪深基金数据
    get_index():沪深指数列表
    :return: None
    """
    dcm_sql = dcm()
    conn = dcm_sql.getconn()

    dfm_db_stocks = df2db.get_all_stocklist()
#    print(dfm_db_stocks)
    dfm_db_stocks['symbol'] = dfm_db_stocks['Market_ID'] + dfm_db_stocks['Stock_ID']

    dfm_stocklist = pd.concat([mt.get_shse(),
                               mt.get_szse(),
                               mt.get_dce(),
                               mt.get_shfe(),
                               mt.get_cffex(),
                               mt.get_fund(),
                               mt.get_index(),
                               mt.get_etf(),])
#    dfm_stocklist.to_excel('stocklist.xls')

    timestamp = datetime.now()

    ls_ins_pars =[]
    ls_upt_pars =[]
    for symbol in dfm_stocklist.index:
        market_id,stock_id = symbol.split('.')
        tquant_symbol_id = symbol
        market_id = 'SH' if market_id == 'SHSE' else market_id
        market_id = 'SZ' if market_id == 'SZSE' else market_id
        if dfm_stocklist.loc[symbol]['is_active'] == 0 and dfm_stocklist.loc[symbol]['lower_limit']==0:
            is_active = 0   #该symbol已退市
        else:
            is_active = 1
        sec_type = dfm_stocklist.loc[symbol]['sec_type'].astype('str')
        stock_name = dfm_stocklist.loc[symbol]['sec_name']
        margin_ratio = dfm_stocklist.loc[symbol]['margin_ratio']
        multiplier = dfm_stocklist.loc[symbol]['multiplier']
        price_tick = dfm_stocklist.loc[symbol]['price_tick']
        lot_size = 100 if market_id in ('SH','SZ') and sec_type in ('1','2') else None

        if market_id+stock_id in list(dfm_db_stocks['symbol']):
            # update
            ls_upt_pars.append((stock_name,
                                sec_type,
                                is_active,
                                timestamp,
                                global_module_name,
                                margin_ratio,
                                multiplier,
                                price_tick,
                                tquant_symbol_id,
                                lot_size,
                                market_id,
                                stock_id))
            logprint("update stock: %s.%s" %(market_id,stock_id))
        else:
            # insert
            ls_ins_pars.append((market_id,
                                stock_id,
                                stock_name,
                                sec_type,
                                is_active,
                                timestamp,
                                global_module_name,
                                margin_ratio,
                                multiplier,
                                price_tick,
                                tquant_symbol_id,
                                lot_size))
            logprint("insert stock: %s.%s" %(market_id,stock_id))

    if ls_ins_pars:
        ins_str = """INSERT INTO stock_basic_info ([Market_ID]
                                               ,[Stock_ID]
                                               ,[Stock_Name]
                                               ,[sec_type]
                                               ,[is_active]
                                               ,[Created_datetime]
                                               ,[Created_by]
                                               ,[margin_ratio]
                                               ,[multiplier]
                                               ,[price_tick]
                                               ,[Tquant_symbol_ID]
                                               ,[lot_size])
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?) """
        try:
            conn.execute(ins_str,ls_ins_pars)
            logprint('INSERT INTO stock_basic_info is done')
        except:
            raise

    if ls_upt_pars:
        upt_str = """UPDATE stock_basic_info SET Stock_Name = ?,sec_type=?,is_active=?,update_time=?,update_by =?,
                     margin_ratio = ?,multiplier=?,price_tick=?,Tquant_symbol_ID=? , lot_size = ? 
                     WHERE Market_ID = ? AND Stock_ID = ?"""
        try:
            conn.execute(upt_str,ls_upt_pars)
            logprint('UPDATE stock_basic_info is done')
        except:
            raise

    logprint('wait for 10 seconds and start fetch stock name changes program')
    time.sleep(10)
    import R10_sensor.fetch_stock_name_changes_from_Tquant as fetch_stock_name_changes_from_Tquant
    fetch_stock_name_changes_from_Tquant.update_stock_name_changes()

if __name__ =='__main__':
    fetch2DB()