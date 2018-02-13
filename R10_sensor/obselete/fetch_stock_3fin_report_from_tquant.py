import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint

import R90_tquant.getdata as gt

"""
this report use tquant module2's get_financial func to get 3 fin report and store them into sql server
get_financial func return 3 dataframe, the first one is balance sheet, the second is profit, and the
third is cash flow.

once program get this 3 reports, it first check whether in columns in the report are in the ZCFG_character table
if not, it add the new column's name as a new character in this table and alter the related transaction table to add new
column as well.

after all columns are added, then it update the report transaction table accordingly.

3 transaction table, one for each report:
stock_fin_balance
stock_fin_profit
stock_fin_cashflow
"""

#TODO: 1.add error handling and log output
#TODO: 2.如何获得当前的程序名称作为创建人或修改者


def fetch2DB():
    # init step
    fetch2DB.timestamp = datetime.now()

    # step1: get DB connection
    dcm_sql = dcm(echo=False)
    engine = dcm_sql.getengine()
    conn = dcm_sql.getconn()

    # step2.1: get current stock list
    dfm_stocks = pd.read_sql_query('''select [Stock_ID] from stock_basic_info 
                                        where (Market_ID = 'SH' or Market_ID = 'SZ') 
                                        and (Stock_ID like '0%' or Stock_ID like '3%' or Stock_ID like '6%')

                                            '''
#                                     + " and Stock_ID = '300274'"  #only used for debug and test purpose
                               , engine)
#    print(dfm_stocks)

    # step2:loop at stock list and fetch and save to DB
    for item in dfm_stocks['Stock_ID']:         # get column Stock_ID from dataframe
    # step2.2: get current character list in each loop so that new chars are included.
        dfm_cur_chars = pd.read_sql_query('''select * from ZCFG_character
                                            where Char_Origin = 'Tquant'
                                            and (Char_Usage = 'FIN10' or Char_Usage = 'FIN20' or Char_Usage = 'FIN30' )
                                                '''
                                          , engine)
        fetch2DB_individual(item,dfm_cur_chars,conn)               # item is str type


def fetch2DB_individual(item : str,dfm_cur_chars,conn):
    logprint('Processing:',item)
    try:
        ls_finreports = gt.get_financial(item)
    except:
        logprint("Stock %s can't get financial information" %item)
        return

    # check finreports columns and update ZCFG_character and alter transaction table

    #apply special process for column name, such as add [] in the column name
    ls_balance_cols = list(map(special_process_col_name,ls_finreports[0].columns))
    ls_profit_cols = list(map(special_process_col_name,ls_finreports[1].columns))
    ls_cashflow_cols = list(map(special_process_col_name,ls_finreports[2].columns))

    #rename the df with new cols name,use rename function with a dict of old column to new column mapping
    df_balance = ls_finreports[0].rename(columns = dict(zip(ls_finreports[0].columns,ls_balance_cols)))
    df_profit  = ls_finreports[1].rename(columns = dict(zip(ls_finreports[1].columns,ls_profit_cols)))
    df_cashflow= ls_finreports[2].rename(columns = dict(zip(ls_finreports[2].columns,ls_cashflow_cols)))


    # print(ls_cashflow_cols )

    """
    datafram的基本功能测试:
    print(dfm_cur_chars,'\n-----')
    print(dfm_cur_chars['Char_Name'],type(dfm_cur_chars['Char_Name']),'\n-----')
    print(dfm_cur_chars['Char_Name','Char_ID'],'\n-----')
    print(dfm_cur_chars[['Char_Name','Char_ID']],'\n-----')
    print(dfm_cur_chars[dfm_cur_chars.Char_Usage == 'FIN10'],'\n-----')
    print(dfm_cur_chars[['Char_Name','Char_ID']][dfm_cur_chars.Char_Usage == 'FIN10'],'\n-----')
    print(dfm_cur_chars['Char_Name'][dfm_cur_chars.Char_Usage == 'FIN10'],'\n-----')
    """


    ls_balance_cols_newadded = [x for x in ls_balance_cols if x not in
                                list(dfm_cur_chars['Char_ID'][dfm_cur_chars.Char_Usage == 'FIN10'])]

    ls_profit_cols_newadded = [x for x in ls_profit_cols if x not in
                                list(dfm_cur_chars['Char_ID'][dfm_cur_chars.Char_Usage == 'FIN20'])]

    ls_cashflow_cols_newadded = [x for x in ls_cashflow_cols if x not in
                                list(dfm_cur_chars['Char_ID'][dfm_cur_chars.Char_Usage == 'FIN30'])]

    # print(ls_balance_cols_newadded)
    # print(ls_profit_cols_newadded)
    # print(ls_cashflow_cols_newadded)

    #step3: insert to char master data and alter transaction table adding new columns
    # 一次性对表加入多列的sql 语句语法:ALTER TABLE stock_fin_balance ADD 测试1 decimal(18,2), 测试2 decimal(18,2)

    # use transaction to commit all in one batch
    trans = conn.begin()
    try:
        if ls_balance_cols_newadded:
            alter_str = "ALTER TABLE stock_fin_balance ADD "
            for newcol in ls_balance_cols_newadded:
                alter_str += newcol + " decimal(18,2),"
                logprint("new column added in table stock_fin_balance:", newcol)
            alter_str = alter_str[:-1]
            # print(alter_str)
            conn.execute(alter_str)

        if ls_profit_cols_newadded:
            alter_str = "ALTER TABLE stock_fin_profit ADD "
            for newcol in ls_profit_cols_newadded:
                alter_str += newcol + " decimal(18,2),"
                logprint("new column added in table stock_fin_profit:", newcol)
            alter_str = alter_str[:-1]
            # print(alter_str)
            conn.execute(alter_str)

        if ls_cashflow_cols_newadded:
            alter_str = "ALTER TABLE stock_fin_cashflow ADD "
            for newcol in ls_cashflow_cols_newadded:
                alter_str += newcol + " decimal(18,2),"
                logprint("new column added in table stock_fin_cashflow:", newcol)
            alter_str = alter_str[:-1]
            # print(alter_str)
            conn.execute(alter_str)

        ls_new_chars = []
        for char in ls_balance_cols_newadded:
            ls_new_chars.append(('Tquant','FIN10',char,char,fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant'))
        for char in ls_profit_cols_newadded:
            ls_new_chars.append(('Tquant','FIN20',char,char,fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant'))
        for char in ls_cashflow_cols_newadded:
            ls_new_chars.append(("Tquant",'FIN30',char,char,fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant'))

        if ls_new_chars:
            ins_str = "INSERT INTO ZCFG_character (Char_Origin,Char_Usage,Char_ID,Char_Name,Created_datetime,Created_by) VALUES (?,?,?,?,?,?)"
            conn.execute(ins_str,tuple(ls_new_chars))

        trans.commit()
    except:
        trans.rollback()
        raise

    #step4: insert transaction data into to 3 transaction table
    market_id = 'SH' if item.startswith('6') else 'SZ'

    #balance report
    dfm_cur_balance = pd.read_sql_query('''select * from stock_fin_balance
                                    where Market_ID = ? and Stock_ID = ?
                                            '''
                                      , conn, params = (market_id,item),index_col='Report_Date')


    ins_str_cols =''
    ins_str_pars =''
    ls_ins_pars =[]
    for ts_id in df_balance.index:
        if ts_id in dfm_cur_balance.index:
            # update logic: only update the value changed cols
            ls_upt_cols =[]
            ls_upt_pars =[]
            for col in df_balance.columns:
                # df_balance的列名之前都加上了[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                tmp_colname = col.replace('[','').replace(']','')
                if tmp_colname in dfm_cur_balance.columns and abs(df_balance.loc[ts_id.date()][col] - dfm_cur_balance.loc[ts_id][tmp_colname]) > 0.01:
                    ls_upt_cols.append(special_process_col_name(tmp_colname) + '=?')
                    ls_upt_pars.append(df_balance.loc[ts_id.date()][col])
                    logprint("Update Balance Stock %s Period %s Column %s from %.2f to %.2f"
                             %(item,ts_id.date(),tmp_colname,dfm_cur_balance.loc[ts_id][tmp_colname],df_balance.loc[ts_id.date()][col]))

            if ls_upt_cols:
                upt_str = ",".join(ls_upt_cols) + ", Last_modified_datetime = ?,Last_modified_by=?"
                ls_upt_pars.extend([fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant', market_id,item,datetime.strptime(str(ts_id.date()),'%Y-%m-%d')])
                update_str = '''UPDATE stock_fin_balance SET %s 
                    WHERE Market_ID = ? AND Stock_ID = ? AND Report_Date = ? ''' %upt_str
                conn.execute(update_str, tuple(ls_upt_pars))
            continue
        # insert logic
        logprint('Insert Balance report stock %s Period %s' %(item,ts_id.date()))
        ins_str_cols= ','.join(df_balance.columns)
        ins_str_pars= ','.join('?'*len(df_balance.columns))
#        print(ins_str_cols)
#        print(ins_str_pars)
        #convert into datetime type so that it can update into SQL server
        report_date = datetime.strptime(str(ts_id.date()),'%Y-%m-%d')
        ls_ins_pars.append((market_id,item,report_date,fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant')
                           +tuple(df_balance.loc[ts_id.date()]))

    if ins_str_cols:
        ins_str = '''INSERT INTO stock_fin_balance (Market_ID,Stock_ID,Report_Date,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,%s)''' %(ins_str_cols,ins_str_pars)
        # print(ins_str)
        conn.execute(ins_str, ls_ins_pars)

    #profit report
    dfm_cur_profit = pd.read_sql_query('''select * from stock_fin_profit
                                    where Market_ID = ? and Stock_ID = ?
                                            '''
                                        , conn, params=(market_id, item), index_col='Report_Date')

    ins_str_cols = ''
    ins_str_pars = ''
    ls_ins_pars = []
    for ts_id in df_profit.index:
        if ts_id in dfm_cur_profit.index:
            # update logic
            # update logic: only update the value changed cols
            ls_upt_cols =[]
            ls_upt_pars =[]
            for col in df_profit.columns:
                # df_balance的列名之前都加上了[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                tmp_colname = col.replace('[','').replace(']','')
                if tmp_colname in dfm_cur_profit.columns and abs(df_profit.loc[ts_id.date()][col] - dfm_cur_profit.loc[ts_id][tmp_colname]) > 0.01:
                    ls_upt_cols.append(special_process_col_name(tmp_colname) + '=?')
                    ls_upt_pars.append(df_profit.loc[ts_id.date()][col])
                    logprint("Update Profit Stock %s Period %s Column %s from %.2f to %.2f"
                             %(item,ts_id.date(),tmp_colname,dfm_cur_profit.loc[ts_id][tmp_colname],df_profit.loc[ts_id.date()][col]))

            if ls_upt_cols:
                upt_str = ",".join(ls_upt_cols) + ", Last_modified_datetime = ?,Last_modified_by=?"
                ls_upt_pars.extend([fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant', market_id,item,datetime.strptime(str(ts_id.date()),'%Y-%m-%d')])
                update_str = '''UPDATE stock_fin_profit SET %s 
                    WHERE Market_ID = ? AND Stock_ID = ? AND Report_Date = ? ''' %upt_str
                conn.execute(update_str, tuple(ls_upt_pars))
            continue
        # insert logic
        logprint('Insert Profit report stock %s Period %s' % (item, ts_id.date()))
        ins_str_cols = ','.join(df_profit.columns)
        ins_str_pars = ','.join('?' * len(df_profit.columns))
        #        print(ins_str_cols)
        #        print(ins_str_pars)
        # convert into datetime type so that it can update into SQL server
        report_date = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
        ls_ins_pars.append((market_id, item, report_date, fetch2DB.timestamp, 'fetch_stock_3fin_report_from_tquant')
                           + tuple(df_profit.loc[ts_id.date()]))

    if ins_str_cols:
        ins_str = '''INSERT INTO stock_fin_profit (Market_ID,Stock_ID,Report_Date,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,%s)''' %(ins_str_cols,ins_str_pars)
        # print(ins_str)
        conn.execute(ins_str, ls_ins_pars)

    #cashflow report
    dfm_cur_cashflow = pd.read_sql_query('''select * from stock_fin_cashflow
                                    where Market_ID = ? and Stock_ID = ?
                                            '''
                                        , conn, params=(market_id, item), index_col='Report_Date')

    ins_str_cols = ''
    ins_str_pars = ''
    ls_ins_pars = []
    for ts_id in df_cashflow.index:
        if ts_id in dfm_cur_cashflow.index:
            # update logic
            # update logic: only update the value changed cols
            ls_upt_cols =[]
            ls_upt_pars =[]
            for col in df_cashflow.columns:
                # df_balance的列名之前都加上了[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                tmp_colname = col.replace('[','').replace(']','')
                if tmp_colname in dfm_cur_cashflow.columns and abs(df_cashflow.loc[ts_id.date()][col] - dfm_cur_cashflow.loc[ts_id][tmp_colname]) > 0.01:
                    ls_upt_cols.append(special_process_col_name(tmp_colname) + '=?')
                    ls_upt_pars.append(df_cashflow.loc[ts_id.date()][col])
                    logprint("Update cashflow Stock %s Period %s Column %s from %.2f to %.2f"
                             %(item,ts_id.date(),tmp_colname,dfm_cur_cashflow.loc[ts_id][tmp_colname],df_cashflow.loc[ts_id.date()][col]))

            if ls_upt_cols:
                upt_str = ",".join(ls_upt_cols) + ", Last_modified_datetime = ?,Last_modified_by=?"
                ls_upt_pars.extend([fetch2DB.timestamp,'fetch_stock_3fin_report_from_tquant', market_id,item,datetime.strptime(str(ts_id.date()),'%Y-%m-%d')])
                update_str = '''UPDATE stock_fin_cashflow SET %s 
                    WHERE Market_ID = ? AND Stock_ID = ? AND Report_Date = ? ''' %upt_str
                conn.execute(update_str, tuple(ls_upt_pars))
            continue
        # insert logic
        logprint('Insert Cashflow report stock %s Period %s' % (item, ts_id.date()))
        ins_str_cols = ','.join(df_cashflow.columns)
        ins_str_pars = ','.join('?' * len(df_cashflow.columns))
        #        print(ins_str_cols)
        #        print(ins_str_pars)
        # convert into datetime type so that it can update into SQL server
        report_date = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
        ls_ins_pars.append((market_id, item, report_date, fetch2DB.timestamp, 'fetch_stock_3fin_report_from_tquant')
                           + tuple(df_cashflow.loc[ts_id.date()]))

    if ins_str_cols:
        ins_str = '''INSERT INTO stock_fin_cashflow (Market_ID,Stock_ID,Report_Date,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,%s)''' %(ins_str_cols,ins_str_pars)
        # print(ins_str)
        conn.execute(ins_str, ls_ins_pars)



def special_process_col_name(tempstr:str):
    """
    :param tempstr:
    :return: str
    特殊处理:由于sql server的列名中对空格,逗号等特殊字符有限制,无法作为列名,故在所有列名前后加上方括号,以保证该列可以顺利建出.
    """
    tempstr = tempstr.replace("[","")
    tempstr = tempstr.replace("]","")
    tempstr = '['+ tempstr+ ']'
    return tempstr

if __name__ == '__main__':
    fetch2DB()