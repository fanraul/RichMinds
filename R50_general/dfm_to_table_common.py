import R50_general.general_constants
from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import pandas as pd
from pandas import Series, DataFrame,Timedelta
import numpy as np
import R50_general.general_helper_funcs as gcf
from datetime import datetime
import time

#initial steps:
# step1: get DB connection
dcm_sql = dcm(echo=False)
conn = dcm_sql.getconn()

def get_cn_stocklist(stock :str ="", ls_excluded_stockids=[]) -> DataFrame:
    """
    获得沪市和深市的股票清单
    :return: dataframe of SH and SZ stocks
    """
    if stock == "":
        dfm_stocks = pd.read_sql_query('''select Market_ID,Stock_ID,Stock_Name,is_active,
                                            Market_ID + Stock_ID as MktStk_ID,
                                            Tquant_symbol_ID,
                                            上市日期
                                            from BD_L1_00_cn_stock_code_list 
                                            where ((Market_ID = 'SH' and Stock_ID like '6%') 
                                            or (Market_ID = 'SZ' and (Stock_ID like '0%' or Stock_ID like '3%' )))
                                            and sec_type = '1' '''
                                       # +  'and Stock_ID not in (%s)' %str_excluded_stockids  //don't filter sotck id
                                       # in SQL language, since it might be thousand entry to be exclued, it will cause
                                       # SQL server memory issue since the DB server currently only 2 GB memory!!
                                   , conn)
        # print(dfm_stocks)
        dfm_stocks_excluded = dfm_stocks[dfm_stocks.Stock_ID.isin(ls_excluded_stockids)]

        dfm_stocks = gcf.dfm_A_minus_B(dfm_stocks,dfm_stocks_excluded,key_cols=['Stock_ID'])
    else:
        dfm_stocks = pd.read_sql_query('''select Market_ID,Stock_ID,Stock_Name,is_active,
                                            Market_ID + Stock_ID as MktStk_ID,
                                            Tquant_symbol_ID,
                                            上市日期                                             
                                            from BD_L1_00_cn_stock_code_list
                                                where  (Market_ID = 'SH' or Market_ID = 'SZ')  
                                                and sec_type = '1'
                                                    ''' + "and Stock_ID = '%s'" %stock
                                       , conn)
    return dfm_stocks

def get_all_stocklist(stock :str ="") -> DataFrame:
    """
    获得所有stocklist中的stock清单,不单单是股票
    :return: dataframe of SH and SZ stocks
    """
    if stock == "":
        dfm_stocks = pd.read_sql_query('''select * from stock_basic_info   
                                                '''
                                   , conn)
       # print(dfm_stocks)
    else:
        dfm_stocks = pd.read_sql_query('''select * from stock_basic_info 
                                                where   
                                                    ''' + "and Stock_ID = '%s'" %stock
                                       , conn)
    return dfm_stocks

def get_lastest_snapshot_from_mtkstk_hist_table(table_name):
    """
    this func call store procedure 'CFM_get_lastest_snapshot_from_mtkstk_hist_table' to get the lastest records (eg.
    one stock id has multiple lines, only the latest lines are return).
    :param table_name: the table name to fetch
    :return: dfm of the latest data
    """
    params = {'table_name':table_name}

    return exec_store_procedure(conn,'CFM_get_lastest_snapshot_from_mtkstk_hist_table',params)

def exec_store_procedure(conn, proc_name, params):
    """
    general function to call a store procedure
    :param conn: connection
    :param proc_name: procedure name
    :param params: a dictionary to hold the parameters
    :return: dataframe of result dataset

    """
    # solution1: this one not good, no column name returned, only the result dataset return
    # return a list of tuple, each tuple represent a db record.
    # sql_params = ",".join(["@{0}={1}".format(name, value) for name, value in params.items()])
    # sql_string = """
    #     DECLARE @return_value int;
    #     EXEC    @return_value = [dbo].[{proc_name}] {params};
    #     SELECT 'Return Value' = @return_value;
    # """.format(proc_name=proc_name, params=sql_params)
    # return conn.execute(sql_string).fetchall()

    # solution2: use pandas read_sql_query, return a dataframe with column name
    sql_params = ",".join(["@{0}={1}".format(name, value) for name, value in params.items()])
    query = 'EXEC {proc_name} {params}'.format(proc_name=proc_name, params=sql_params)
    dfm_resultset = pd.read_sql_query(query, conn)
    # gcf.dfmprint(df)
    return dfm_resultset

def get_data_from_DB(table_name, dfm_conditions=DataFrame(), oper_dfm = 'AND', free_conditions :str ="") -> DataFrame:
    """
    dfm_conditions: dfm structure,  db_col-> col name, db_oper-> operater (=,>,<,like etc.), db_val->condition value,
    free_conditions: any condition sentence
    return: dataframe of results
    """
    ls_dfm_cond = []
    str_dfm_cond = ''
    if len(dfm_conditions) > 0:
        for index,row in dfm_conditions.iterrows():
            ls_dfm_cond.append(row['db_col'] + ' ' + row['db_oper'] + ' '+ row['db_val'])
        str_oper = ' ' + oper_dfm + ' '
        str_dfm_cond = str_oper.join(ls_dfm_cond)

    if str_dfm_cond:
        if free_conditions:
            dfm_data = pd.read_sql_query('select * from %s where %s' %(table_name,str_dfm_cond + ' AND '+free_conditions)
                                   , conn)
            return dfm_data
        else:
            dfm_data = pd.read_sql_query('select * from %s where %s' %(table_name,str_dfm_cond)
                                   , conn)
            return dfm_data
    else:
        if free_conditions:
            dfm_data = pd.read_sql_query('select * from %s where %s' %(table_name,free_conditions)
                                   , conn)
            return dfm_data
        else:
            dfm_data = pd.read_sql_query('select * from %s' %table_name
                                   , conn)
            return dfm_data


def get_chars(origin = '',usages = [],freq=['D'],charids=[]) ->DataFrame:
    """
    获得股票的属性chars清单
    :param origin:
    :param usages: 可以传入一个list,包含多个值,但是不支持模糊查询
    :param frq:
    :param charids:可以传入一个list,包含多个值,但是不支持模糊查询
    :return:
    """

    ls_sel_str =[]
    sel_str_orgin = "Char_Origin = '%s'" %origin if origin else ''
    sel_str_usage = "Char_Usage in ('%s')" %"','".join(usages) if usages else ''
    sel_str_freq = "Char_Freq in ('%s')" %"','".join(freq) if freq else ''
    sel_str_charids = "Char_ID in ('%s')" %"','".join(charids) if charids else ''
    sel_str = 'select * from ZCFG_character %s' %concatenate_sel_str(sel_str_orgin,sel_str_usage,sel_str_freq,sel_str_charids)
#    print(sel_str)
    dfm_cur_chars = pd.read_sql_query( sel_str
                                      , conn)

    return dfm_cur_chars

def concatenate_sel_str(*args):
    ls_sel_str = [x for x in args if x]
    if ls_sel_str:
        return 'WHERE ' + ' AND '.join(ls_sel_str)
    else:
        return ''

def create_table_by_template(table_name:str,table_type:str):
    dfm_table_check = pd.read_sql_query(" select * from sys.objects where type = 'U' and name = '%s'" % table_name.strip(),
                                        conn)
    if len(dfm_table_check) == 0:
        if table_type == 'stock_date':
            crt_str = R50_general.general_constants.dbtemplate_stock_date % {'table':table_name.strip()}
        elif table_type == 'stock_date_multi_value':
            crt_str = R50_general.general_constants.dbtemplate_stock_date_multi_value % {'table':table_name.strip()}
        # elif table_type == 'index_date_multi_value':
        #     crt_str = R50_general.general_constants.dbtemplate_index_date_multi_value % {'table':table_name.strip()}
        elif table_type == 'stock_wo_date':
            crt_str = R50_general.general_constants.dbtemplate_stock_wo_date % {'table':table_name.strip()}
        elif table_type == 'catg_date':
            crt_str = R50_general.general_constants.dbtemplate_catg_date % {'table': table_name.strip()}
        elif table_type == 'catg_date_multi_value_futuquant':
            crt_str = R50_general.general_constants.dbtemplate_catg_date_multi_value_futuquant % {'table': table_name.strip()}
        elif table_type == 'jd_newslist':
            crt_str = R50_general.general_constants.dbtemplate_jd_newslist % {'table': table_name.strip()}
        else:
            assert 0==1, 'Non-known table type:%s' %table_type
        conn.execute(crt_str)
        logprint('Table %s is created' %table_name)

def add_new_chars_and_cols(dict_cols_cur:dict,ls_cols_db:list,table_name:str,dict_misc_pars:dict):
    """
    do 2 things:
    1) add new cols as new chars into table ZCFG_character
    2) alter related transaction table adding new cols
    :param dict_cols_cur: cols in dataframe got from API or web, key: value -> col name: data type
    :param ls_cols_db: cols in database, each col is a char in ZCFG_character
    :return: N/A
    """
    ls_cols_newadded = [x.strip() for x in dict_cols_cur.keys() if x.strip() not in ls_cols_db]
    #to be a col name in SQL server, there are some rules, so special process is required.
    ls_db_col_name_newadded = list(map(special_process_col_name,ls_cols_newadded))

    # insert to char master data and alter transaction table adding new columns
    # 一次性对表加入多列的sql 语句语法:ALTER TABLE stock_fin_balance ADD 测试1 decimal(18,2), 测试2 decimal(18,2)

    # use transaction to commit all in one batch
    trans = conn.begin()
    timestamp = datetime.now()

    try:
        if ls_db_col_name_newadded:
            alter_str = "ALTER TABLE %s ADD " %table_name
            for i in range(len(ls_db_col_name_newadded)):
                alter_str += "%(col_name)s %(data_type)s," %{'col_name':ls_db_col_name_newadded[i],
                                                             'data_type': dict_cols_cur[ls_cols_newadded[i]]}
                logprint("new column added in table %s:" %table_name, ls_db_col_name_newadded[i])
            alter_str = alter_str[:-1]
            print(alter_str)
            conn.execute(alter_str)

        ls_new_chars = []
        for i in range(len(ls_cols_newadded)):
            ls_new_chars.append((dict_misc_pars['char_origin'],
                                 dict_misc_pars['char_usage'],
                                 dict_misc_pars['char_freq'],
                                 ls_cols_newadded[i],
                                 ls_cols_newadded[i],
                                 dict_misc_pars['allow_multiple'],
                                 table_name,
                                 ls_db_col_name_newadded[i],
                                 timestamp,
                                 dict_misc_pars['created_by']))

        if ls_new_chars:
            ins_str = """INSERT INTO ZCFG_character 
                        (Char_Origin,Char_Usage,Char_Freq,Char_ID,Char_Name,Allow_multiple,Data_table,
                        Data_column,Created_datetime,Created_by)
                         VALUES (?,?,?,?,?,?,?,?,?,?)"""

            conn.execute(ins_str,tuple(ls_new_chars))

        trans.commit()
    except:
        trans.rollback()
        raise

def load_dfm_to_db_single_value_by_mkt_stk_w_hist(market_id, item, dfm_data:DataFrame, table_name:str, dict_misc_pars:dict,
                                                  processing_mode:str = 'w_update',enable_delete = True,partial_ind = False,
                                                  float_fix_decimal = 4):
    """
    本函数用于单值属性的char的历史数据更新.
    本函数只支持单值类型的chars的表更新,如果char是多值的,请用load_dfm_to_db_multi_value_by_mkt_stk_w_hist(尚未开发)
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: dfm包含历史数据,如不存在,则insert,存在并且数据发生了变化,就update
    2) "wo_update: dfm包含历史数据,如不存在,则insert,但是不做update(效率更高)
    注意:传入的datafram中的index必须是时间.
    :param market_id:
    :param stock_id:
    :param dfm:
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :enable_delete: True-> trigger the consistent check for deletion,
                    False-> no action for deletion check
    partial_ind: Ture-> means the dfm only contain the lastest certain period data, mean not cover all time period
                 False-> means the dfm covers full time period.
                 if this indicator is True, then DB data extraction will be limited to the earlist datetime of the dfm.
                 The entries before the earlist datetime won't be extracted or check or any other action.
    float_fix_decimal: since float type data is not accurate, eg. 1.1 actually is 1.0999999999997, so to compare whether
                        the value of dfm is different from the value of db, we need to have a fix decimal designed, then
                        only combine the float based on is fixed decimal, default 4 means if dif is smaller than 0.0001,
                        then we consider two float is the same.
    :return:
    """
    # load DB contents
    dt_key_cols = {'Market_ID':market_id,'Stock_ID':item}

    load_dfm_to_db_single_value_by_key_cols_w_hist(
        dt_key_cols, dfm_data, table_name, dict_misc_pars, processing_mode,enable_delete,partial_ind,float_fix_decimal)


def load_dfm_to_db_single_value_by_key_cols_w_hist(dt_key_cols:dict,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                                   processing_mode:str,enable_delete,partial_ind,float_fix_decimal):
    """
    本函数用于单值属性的char的历史数据更新.
    每组关键字需调用一次本函数,即如果要更新100行数据(100个不同的key值),需要调用100次本函数,本程序每次处理一行.
    本函数只支持单值类型的chars的表更新,如果char是多值的,请用load_dfm_to_db_multi_value_by_key_cols_w_hist(尚未开发)
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: dfm包含历史数据,如不存在,则insert,存在并且数据发生了变化,就update
    2) "wo_update: dfm包含历史数据,如不存在,则insert,如存在,则不处理(效率更高)
    delete的处理,目前考虑先将
    注意:传入的datafram中的index必须是时间.
    :param dt_key_cols: a dictionary of key col name and key col value. 比如marketid和stockid作为key值,则例子如下:
                        {'Market_ID':'SH','Stock_ID':'600000'},时间是db table中的key,但是在本函数中不作为key_cols
    :param dfm: datafram中的index必须是时间
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :return:
    """
    # TODO: delete的处理,目前作为inconsistency,看实际的发生情况再做处理

    # load DB contents
    timestamp = datetime.now()

    ls_key_col_items = dt_key_cols.items()
    ls_sel_key_cols = [ x[0] + ' = ?' for x in ls_key_col_items]
    ls_sel_key_cols_value = [x[1] for x in ls_key_col_items]

    str_key_cols = ','.join([x[0] for x in ls_key_col_items])
    str_sel_key_cols = ' and '.join(ls_sel_key_cols)

    if partial_ind:
        set_cur_trans_datetimes = set(dfm_data.index)
        oldest_trans_datetime = min(set_cur_trans_datetimes)
    else:
        oldest_trans_datetime = datetime(1900,1,1)
    dfm_db_data = pd.read_sql_query("select * from %s where %s AND Trans_Datetime >= ?" %(table_name,str_sel_key_cols)
                                        , conn, params=(*ls_sel_key_cols_value,oldest_trans_datetime), index_col='Trans_Datetime')

    # gcf.dfmprint(dfm_db_data)
    # gcf.dfmprint(dfm_data)
    ins_str_cols = ''
    ins_str_pars = ''
    ls_ins_pars = []
    for ts_id in dfm_data.index:
        if ts_id in dfm_db_data.index:
            #entry already exist
            if processing_mode == 'w_update':
                # update logic: only update the value changed cols
                ls_upt_cols = []
                ls_upt_pars = []
                for col in dfm_data.columns:
                    # dfm_data的列名有可能带[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                    tmp_colname = col.replace('[', '').replace(']', '')
                    if tmp_colname in dfm_db_data.columns:
                        # 如果两种是空值,只是None的类型不同,不处理.
                        # print('hello',dfm_data.loc[ts_id][col],',', dfm_db_data.loc[ts_id][tmp_colname])
                        if pd.isnull(dfm_data.loc[ts_id][col]) and pd.isnull(dfm_db_data.loc[ts_id][tmp_colname]):
                            continue
                        if dfm_data.loc[ts_id][col] != dfm_db_data.loc[ts_id][tmp_colname]:
                            # print(dfm_data.loc[ts_id][col],type(dfm_data.loc[ts_id][col]))
                            # print(dfm_db_data.loc[ts_id][tmp_colname],type(dfm_db_data.loc[ts_id][tmp_colname]))
                            #if numpy.float64 then it is possible the value has slight difference,so change rule to allow dif <0.0001
                            dif = 1./10**float_fix_decimal
                            try:
                                dif = abs(dfm_data.loc[ts_id][col]- dfm_db_data.loc[ts_id][tmp_colname])
                            except:
                                dif = 1
                            if isinstance(dif, Timedelta) or dif >= 1. / 10**float_fix_decimal:
                                ls_upt_cols.append(special_process_col_name(tmp_colname) + '=?')
                                # convert numpy type to stanard data type if required.
                                ls_upt_pars.append(pandas_numpy_type_convert_to_standard(dfm_data.loc[ts_id][col]))
                                logprint("Update %s %s Period %s Column %s from %s to %s"
                                         % (table_name,dt_key_cols, ts_id, tmp_colname, dfm_db_data.loc[ts_id][tmp_colname],
                                            dfm_data.loc[ts_id][col]))
                    else:
                        logprint("Column num %s doesn't exist in table %s" %(tmp_colname,table_name))

                if ls_upt_cols:
                    upt_str = ",".join(ls_upt_cols) + ", Last_modified_datetime = ?,Last_modified_by=?"

                    ls_upt_pars.extend([timestamp, dict_misc_pars['update_by'], *ls_sel_key_cols_value,
                                        datetime.strptime(str(ts_id.date()), '%Y-%m-%d')])

                    update_str = '''UPDATE %s SET %s 
                        WHERE %s AND Trans_Datetime = ? ''' %(table_name,upt_str,str_sel_key_cols )
                    conn.execute(update_str, tuple(ls_upt_pars))
            elif processing_mode == 'wo_update':
                continue
            else:
                assert 0 == 1,'processing_mode %s unkown, please double check!' %processing_mode
            continue
        # insert logic
        logprint('Insert %s %s Period %s' % (table_name,dt_key_cols, ts_id.date()))
        # rename the df with new cols name,use rename function with a dict of old column to new column mapping
        ls_colnames_dbinsert = list(map(special_process_col_name,dfm_data.columns))
        ins_str_cols = ','.join(ls_colnames_dbinsert)
        ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
        #        print(ins_str_cols)
        #        print(ins_str_pars)
        # convert into datetime type so that it can update into SQL server
#        trans_datetime = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
        trans_datetime = ts_id.to_pydatetime()
        ls_dfmrow_dbtype_aligned = dbtype_aligned_format(dfm_data.loc[ts_id])
        ls_ins_pars.append((*ls_sel_key_cols_value, trans_datetime, timestamp, dict_misc_pars['update_by'])
                           + tuple(ls_dfmrow_dbtype_aligned))

    if ins_str_cols:
        ins_str = '''INSERT INTO %s (%s,Trans_Datetime,Created_datetime,Created_by,%s) VALUES (?,?,?,?,?,%s)''' % (
            table_name,str_key_cols,ins_str_cols, ins_str_pars)
        # print(ins_str)
        try:
            for ins_par in ls_ins_pars:
                conn.execute(ins_str, ins_par)
            # conn.execute(ins_str, ls_ins_pars)
        except:
            raise

    # handle delete case
    if enable_delete:
        for ts_id in dfm_db_data.index:
            if ts_id not in dfm_data.index:
                logprint('Inconsistency Found! %s %s Period %s in DB but not found in data source.' % (table_name,dt_key_cols, ts_id.date()),add_log_files='I')
                logprint('No DB update happens in this case, please manually handle it',add_log_files='I')

def dbtype_aligned_format(series_tmp):
    # some data types in Dataframe: eg. Numpy.int32 doesn't support by SQlAlchemy, so need to convert to Python general
    # data type, then update DB.
    # conversion rule:
    # numpy.int* -> int
    # numpy.float* -> float
    # numpy.nan or pd.NaT -> None  (use pd.isnull() to check)
    ls_par = []
    for par in series_tmp:
        ls_par.append(pandas_numpy_type_convert_to_standard(par))
    return ls_par

def pandas_numpy_type_convert_to_standard(par):
    if pd.isnull(par):
        return None
    if isinstance(par, np.int32) or isinstance(par, np.int64):
        return int(par)
    elif isinstance(par, np.float32) or isinstance(par, np.float64):
        return float(par)
    else:
        return par

def load_dfm_to_db_multi_value_by_mkt_stk_w_hist(market_id, item, dfm_data: DataFrame, table_name: str,
                                                 dict_misc_pars: dict, processing_mode: str = 'w_update',
                                                 enable_delete = True, partial_ind = False,
                                                 float_fix_decimal =4):

    """
    传入的datafram中的index必须是datetime类型的时间参数,代表trans datetime.
    1) 'w_update: dfm包含历史数据,如不存在,则insert,存在并且数据发生了变化,就update
    2) "wo_update: dfm包含历史数据,如不存在,则insert,如存在,则不处理(效率更高)
    :param market_id:
    :param item:
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :param processing_mode:
    :return:
    """
    dt_key_cols = {'Market_ID':market_id,'Stock_ID':item}
    load_dfm_to_db_multi_value_by_key_cols_w_hist(dt_key_cols,dfm_data,table_name,dict_misc_pars,processing_mode,enable_delete,partial_ind,float_fix_decimal)

def load_dfm_to_db_multi_value_by_key_cols_w_hist(
        dt_key_cols:dict,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,processing_mode:str,enable_delete,partial_ind,float_fix_decimal):
    # TODO: delete的处理,目前作为inconsistency,看实际的发生情况再做处理

    # load DB contents
    timestamp = datetime.now()

    ls_key_col_items = dt_key_cols.items()
    ls_sel_key_cols = [x[0] + ' = ?' for x in ls_key_col_items]
    ls_key_cols_value = [x[1] for x in ls_key_col_items]

    str_key_cols = ','.join([x[0] for x in ls_key_col_items])
    str_sel_key_cols = ' and '.join(ls_sel_key_cols)

    if partial_ind:
        set_cur_trans_datetimes = set(dfm_data.index)
        oldest_trans_datetime = min(set_cur_trans_datetimes)
    else:
        oldest_trans_datetime = datetime(1900,1,1)

    dfm_db_data = pd.read_sql_query("select * from %s where %s AND Trans_Datetime >= ?" % (table_name, str_sel_key_cols)
                                    , conn, params=(*ls_key_cols_value,oldest_trans_datetime), index_col='Trans_Datetime')

    set_cur_unique_tsdate = set(dfm_data.index)
    set_db_unique_tsdate = set(dfm_db_data.index)

    # make sure dfm is index sorted, so that index slice works
    dfm_db_data.sort_index(inplace=True)
    dfm_data.sort_index(inplace=True)

    for cur_uni_tsdate in set_cur_unique_tsdate:
        # use slice to make sure get a dataframe by index
        sub_dfm_data = dfm_data[cur_uni_tsdate:cur_uni_tsdate]
        if cur_uni_tsdate in set_db_unique_tsdate:
            if processing_mode == 'w_update':
                #update
                # check cur value is the same as db value,if not, delete old entry and recreate,
                # compare whether the cur dfm values is the same as db dfm values of latest fetching
                sub_dfm_db_data = dfm_db_data[cur_uni_tsdate:cur_uni_tsdate]
                set_dif_cur2db = gcf.setdif_dfm_A_to_B(sub_dfm_data, sub_dfm_db_data, dfm_data.columns,float_fix_decimal)
                set_dif_db2cur = gcf.setdif_dfm_A_to_B(sub_dfm_db_data, sub_dfm_data, dfm_data.columns,float_fix_decimal)
                if len(set_dif_cur2db) > 0:
                    logprint(
                        'Inconsistency found with db %s update! Entry %s Period %s new value set shows: %s' % (table_name,
                        dt_key_cols,cur_uni_tsdate.date(),set_dif_cur2db),add_log_files='I')
                if len(set_dif_db2cur) > 0:
                    logprint(
                        'Inconsistency found with db %s update! Entry %s Period %s old value set gones: %s' % (table_name,
                        dt_key_cols, cur_uni_tsdate.date(),set_dif_db2cur),add_log_files='I')
                if len(set_dif_cur2db) == 0 and len(set_dif_db2cur) == 0:
                    continue

                # delete DB existing entries
                del_par = [*ls_key_cols_value,cur_uni_tsdate]
                del_str = '''DELETE FROM %s WHERE %s AND Trans_Datetime = ? ''' % (
                    table_name, str_sel_key_cols)
                logprint('Delete %s entry %s Period %s' % (table_name, dt_key_cols, cur_uni_tsdate.date()))
                # print(ins_str)
                try:
                    conn.execute(del_str, del_par)
                except:
                    raise
            elif processing_mode == 'wo_update':
                continue
            else:
                assert 0 == 1, 'processing_mode %s unkown, please double check!' % processing_mode

        #insert the sub dfm into db
        insert_multi_value_dfm_to_db_by_key_cols_transdate(dt_key_cols, cur_uni_tsdate,sub_dfm_data ,
                                                               table_name, dict_misc_pars, timestamp)

    if enable_delete:
        for db_uni_tsdate in set_db_unique_tsdate:
            if db_uni_tsdate not in set_cur_unique_tsdate:
                logprint('Inconsistency Found! %s %s Period %s in DB but not found in data source.' %(table_name,dt_key_cols, db_uni_tsdate.date()),
                         add_log_files='I')
                logprint('No DB update happens in this case, please manually handle it',add_log_files='I')

def load_dfm_to_db_multi_value_by_mkt_stk_cur(market_id,item,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                              process_mode = 'w_check',float_fix_decimal = 4):
    """
    本函数用于多值属性的char的当前数据更新.
    :param market_id:
    :param item:
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :return:
    """
    # load DB contents

    dt_key_cols = {'Market_ID':market_id,'Stock_ID':item}
    load_dfm_to_db_multi_value_by_key_cols_cur(dt_key_cols,dfm_data,table_name,dict_misc_pars,process_mode,float_fix_decimal)


def load_dfm_to_db_multi_value_by_key_cols_cur(dt_key_cols:dict,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                              process_mode,float_fix_decimal):
    """
    本函数用于多值属性的char的当前数据更新.
    1) 如果last_trading_day已经在db中存在了,会进行校验,如果发现不一致(当前数据多次抓取,对应的值有不一样的现象发生),会记录
        inconsistent的日志,但是不会自动进行更新.
    2) 当last_trading_day在db中不存在,则会更新process_model决定是直接更新还是有选择性的更新
        2.1) process_model = 'w_check',则会检dfm中的数据是否和最近的DB记录是否一致,不一致才进行更新,这种模式适合非每天更新的数据,比如
             股票所属板块.
        2.2) 非以上情况,dfm直接更新db,这种模式适合每天都必须更新的数据,比如股票每日的交易信息
    :param market_id:
    :param item:
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :return:
    """
    # load DB contents
    timestamp = datetime.now()

    ls_key_col_items = dt_key_cols.items()
    ls_sel_key_cols = [ x[0] + ' = ?' for x in ls_key_col_items]   # ['Market_ID = ?','Stock_ID = ?']
    ls_key_cols_value = [x[1] for x in ls_key_col_items]       # ['SH','600000']

    str_key_cols = ','.join([x[0] for x in ls_key_col_items])
    str_sel_key_cols = ' and '.join(ls_sel_key_cols)

    last_trading_day = gcf.get_last_trading_day()
    last_trading_daytime = datetime.strptime(str(last_trading_day), '%Y-%m-%d')
    dfm_db_data = pd.read_sql_query("select * from %s where %s order by Trans_Datetime DESC" %(table_name,str_sel_key_cols)
                                        , conn, params=tuple(ls_key_cols_value), index_col='Trans_Datetime')
    # print(dfm_data)
    # print(dfm_db_data)
    if pd.Timestamp(last_trading_day) in dfm_db_data.index:
        # entry already exist in table, since it is current date db insert, no need to update history data already exist
        # TODO: may add logic to allow multiple update per day
        logprint('Table %s entry %s no need to update since only one update per day,but value check to ensure consistency!'
                 %(table_name,dt_key_cols))
        # check cur value is the same as db value,if not, inconsistency found, need manually processing
        #get latest db data set
        set_latest_db_values = dfm_db_data.loc[dfm_db_data.index[0]]
        #compare whether the cur dfm values is the same as db dfm values of latest fetching
        set_dif_cur2db = gcf.setdif_dfm_A_to_B(dfm_data,set_latest_db_values,dfm_data.columns,float_fix_decimal)
        set_dif_db2cur = gcf.setdif_dfm_A_to_B(set_latest_db_values,dfm_data,dfm_data.columns,float_fix_decimal)
        # TODO: error handling
        if len(set_dif_cur2db) > 0:
            logprint('Inconsistency found for multiple fetchs of db %s in one day! Entry %s new value set shows: %s' %(table_name,dt_key_cols,set_dif_cur2db),
                     add_log_files='I')
            logprint('No DB update happens in this case, please manually handle it',add_log_files='I')
        if len(set_dif_db2cur) > 0:
            logprint('Inconsistency found for multiple fetchs of db %s in one day! Entry %s old value set gones: %s' %(table_name,dt_key_cols,set_dif_db2cur),
                     add_log_files='I')
            logprint('No DB update happens in this case, please manually handle it',add_log_files='I')
        return

    if process_mode == 'w_check' and len(dfm_db_data) > 0:
        # only insert DB in case the value is different from latest datetime's values
        # build a set to hold all multi values in dfm_data

        #get latest db data set
        set_latest_db_values = dfm_db_data.loc[dfm_db_data.index[0]]
        #compare whether the cur dfm values is the same as db dfm values of latest fetching
        set_dif_cur2db = gcf.setdif_dfm_A_to_B(dfm_data,set_latest_db_values,dfm_data.columns,float_fix_decimal)
        set_dif_db2cur = gcf.setdif_dfm_A_to_B(set_latest_db_values,dfm_data,dfm_data.columns,float_fix_decimal)

        if set_dif_cur2db:
            logprint('Entry %s new value set appears:' %dt_key_cols,set_dif_cur2db)
        if set_dif_db2cur:
            logprint('Entry %s old value set removed:' %dt_key_cols,set_dif_db2cur)
        if not set_dif_cur2db and not set_dif_db2cur:
            # no need to update, it is the same as last record
            # logprint('Stock %s value set no changes:' % item)
            return

    insert_multi_value_dfm_to_db_by_key_cols_transdate(dt_key_cols,last_trading_daytime, dfm_data,
                                                       table_name, dict_misc_pars, timestamp)

def insert_multi_value_dfm_to_db_by_key_cols_transdate(dt_key_cols:dict,trans_datetime, dfm_data:DataFrame,
                                                       table_name:str,dict_misc_pars:dict,timestamp):

    ls_key_col_items = dt_key_cols.items()
    ls_key_cols_value = [x[1] for x in ls_key_col_items]       # ['SH','600000']
    str_key_cols = ','.join([x[0] for x in ls_key_col_items])
    mark_key_cols = ','.join(['?']*len(dt_key_cols))

    # insert into db
    ls_ins_pars = []
    logprint('Insert %s entry %s Period %s' % (table_name,dt_key_cols, trans_datetime.date()))
    # rename the df with new cols name,use rename function with a dict of old column to new column mapping
    ls_colnames_dbinsert = list(map(special_process_col_name,dfm_data.columns))
    ins_str_cols = ','.join(ls_colnames_dbinsert)
    ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))

    for id in range(len(dfm_data)):
        ls_dfmrow_dbtype_aligned = dbtype_aligned_format(dfm_data.iloc[id])
        ls_ins_pars.append((*ls_key_cols_value, trans_datetime, id, timestamp, dict_misc_pars['update_by'])
                       + tuple(ls_dfmrow_dbtype_aligned))

    if ins_str_cols:
        ins_str = '''INSERT INTO %s (%s,Trans_Datetime,Sqno,Created_datetime,Created_by,%s) VALUES (%s,?,?,?,?,%s)''' % (
            table_name,str_key_cols,ins_str_cols,mark_key_cols, ins_str_pars)
        # print(ins_str)
        try:
            conn.execute(ins_str, ls_ins_pars)
        except:
            raise


def load_dfm_to_db_single_value_by_mkt_stk_cur(market_id,item,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                               process_mode:str='w_check',float_fix_decimal = 4):
    """
    针对单个market和stock组合,char为单值的当前最新记录处理,
    1) 如果last_trading_day已经在db中存在了,直接进行覆盖.
    2) 当last_trading_day在db中不存在,则会更新process_model决定是直接更新还是有选择性的更新
        2.1) process_model = 'w_check',则会检dfm中的数据是否和最近的DB记录是否一致,不一致才进行更新,这种模式适合非每天更新的数据,比如
             股票所属板块.
        2.2) 非以上情况,dfm直接更新db,这种模式适合每天都必须更新的数据,比如股票每日的交易信息
    :param market_id:
    :param item:
    :param dfm_data: 只需包含当前获得的具体char值,无需Trans_Datetime的信息,函数会自动取last transaction day
    :param table_name:
    :param dict_misc_pars:
    :param process_mode:
    :return:
    """

    dt_key_cols = {'Market_ID': market_id, 'Stock_ID': item}
    load_dfm_to_db_single_value_by_key_cols_cur(dt_key_cols, dfm_data, table_name, dict_misc_pars, process_mode,
                                               float_fix_decimal)


def load_dfm_to_db_single_value_by_key_cols_cur(dt_key_cols:dict,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                              process_mode,float_fix_decimal):
    """
    本函数用于单值属性的char的当前数据更新.
    1) 如果处理模式为'w_check',则会检dfm中的数据是否和最近的DB记录是否一致,不一致才进行更新,这种模式适合非每天更新的数据,比如
        股票所属板块.
    2) 非以上情况, 则对dfm增加(key col:'Trans_Datetime',值为last_trading_day),然后使用该dfm直接更新db,这种模式适合每天都必须更新的数据,
        比如股票每日的交易信息,效果是不存在则insert,存在则update.(无inconsistency检查).
    :param market_id:
    :param item:
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :return:
    """
    if process_mode == 'w_check':
        # load DB contents
        timestamp = datetime.now()

        ls_key_col_items = dt_key_cols.items()
        ls_sel_key_cols = [ x[0] + ' = ?' for x in ls_key_col_items]   # ['Market_ID = ?','Stock_ID = ?']
        ls_key_cols_value = [x[1] for x in ls_key_col_items]       # ['SH','600000']

        str_key_cols = ','.join([x[0] for x in ls_key_col_items])
        mark_key_cols = ','.join(['?'] * len(dt_key_cols))
        str_sel_key_cols = ' and '.join(ls_sel_key_cols)

        last_trading_day = gcf.get_last_trading_day()
        last_trading_daytime = datetime.strptime(str(last_trading_day), '%Y-%m-%d')
        dfm_db_data = pd.read_sql_query("select * from %s where %s order by Trans_Datetime DESC" %(table_name,str_sel_key_cols)
                                            , conn, params=tuple(ls_key_cols_value), index_col='Trans_Datetime')
        # print(dfm_data)
        # print(dfm_db_data)
        if pd.Timestamp(last_trading_day) in dfm_db_data.index:
            # if last_trading_day is already in db, meaning several fetches in one day, overwrite with new data
            for key,value in dt_key_cols.items():
                dfm_data[key] = value
            key_cols = list(dt_key_cols.keys())
            load_dfm_to_db_cur(dfm_data, key_cols, table_name, dict_misc_pars, process_mode='w_update')
            return

        ins_flg = False
        if len(dfm_db_data) > 0:
            # only insert DB in case the value is different from latest datetime's values
            s_db_data_max_ts = dfm_db_data.loc[dfm_db_data.index[0]]
            for col in dfm_data.columns:
                # dfm_data的列名有可能带[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                tmp_colname = col.replace('[', '').replace(']', '')
                if tmp_colname in dfm_db_data.columns:
                    # 如果两种是空值,只是None的类型不同,不处理.
                    if pd.isnull(dfm_data.iloc[0][col]) and pd.isnull(s_db_data_max_ts[tmp_colname]):
                        continue
                    if dfm_data.iloc[0][col] != s_db_data_max_ts[tmp_colname]:
                        # if numpy.float64 then it is possible the value has slight difference,so change rule to allow dif <0.0001
                        dif = 1. / 10 ** float_fix_decimal
                        try:
                            dif = abs(dfm_data.iloc[0][col] - s_db_data_max_ts[tmp_colname])
                        except:
                            dif = 1.
                        if isinstance(dif, Timedelta) or dif >= 1. / 10 ** float_fix_decimal:
                            # cur data is different from db data of latest transaction datetime
                            ins_flg = True
                            break
                else:
                    assert 0==1,"Column name %s doesn't exist in table %s but in dataframe" % (tmp_colname, table_name)
        else:
            # no db entry
            ins_flg = True
        if ins_flg:
            # insert logic
            logprint('Insert %s %s Period %s' % (table_name, dt_key_cols, last_trading_day))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbinsert = list(map(special_process_col_name, dfm_data.columns))
            ins_str_cols = ','.join(ls_colnames_dbinsert)
            ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
            ls_dfmrow_dbtype_aligned = dbtype_aligned_format(dfm_data.iloc[0])
            ls_ins_pars = []
            ls_ins_pars.append((*ls_key_cols_value, last_trading_daytime, timestamp, dict_misc_pars['update_by'])
                               + tuple(ls_dfmrow_dbtype_aligned))
            ins_str = '''INSERT INTO %s (%s,Trans_Datetime,Created_datetime,Created_by,%s) VALUES (%s,?,?,?,%s)''' % (
                table_name, str_key_cols, ins_str_cols,mark_key_cols, ins_str_pars)
            # print(ins_str)
            try:
                for ins_par in ls_ins_pars:
                    conn.execute(ins_str, ins_par)
            except:
                raise

    else:
        for key, value in dt_key_cols.items():
            dfm_data[key] = value
        key_cols = list(dt_key_cols.keys())
        load_dfm_to_db_cur(dfm_data,key_cols,table_name,dict_misc_pars,process_mode='w_update')

def load_dfm_to_db_cur(dfm_cur_data:DataFrame,key_cols:list,table_name:str,dict_misc_pars:dict,process_mode:str):
    """
    本函数用于单值属性的char的当前数据更新,注意,更新的表中是有transac_datetime作为key值的.
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: key_cols存在时, 进行update
    2) "wo_update: key_cols存在时,不进行update

    传入的dataframe中无需包含Trans_Datetime,Created_datetime,Created_by,Last_modified_datetime,Last_modified_by这四列.
    trans_datetime是考虑交易所的交易日历的
    Trans_Datetime默认取当前日期.
    :param dfm:
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :return:
    """

    # load DB contents
    timestamp = datetime.now()
    last_trading_day = gcf.get_last_trading_day()
    last_trading_daytime = gcf.get_last_trading_daytime()
    dfm_db_data = pd.read_sql_query("select * from %s where Trans_Datetime = ?" %table_name
                                        , conn, params=(last_trading_daytime,))
    dfm_cur_data['Trans_Datetime'] = last_trading_daytime
    dfm_insert_data = gcf.dfm_A_minus_B(dfm_cur_data,dfm_db_data,key_cols)
    dfm_update_data = gcf.dfm_A_intersect_B(dfm_cur_data,dfm_db_data,key_cols)

    # gcf.dfmprint(dfm_insert_data)
    if len(dfm_insert_data) > 0:
        ins_str_cols = ''
        ins_str_pars = ''
        ls_ins_pars = []
        for index,row in dfm_insert_data.iterrows():
            # insert logic
            logprint('Insert table %s new Record %s at Period %s' % (table_name, [row[col] for col in key_cols] , last_trading_day))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbinsert = list(map(special_process_col_name,dfm_insert_data.columns))
            ins_str_cols = ','.join(ls_colnames_dbinsert)
            ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
            #        print(ins_str_cols)
            #        print(ins_str_pars)
            # convert into datetime type so that it can update into SQL server
            # trans_datetime = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
            # trans_datetime = ts_id.to_pydatetime()
            ls_dfmrow_dbtype_aligned = dbtype_aligned_format(row)
            ls_ins_pars.append((timestamp, dict_misc_pars['created_by'])
                               + tuple(ls_dfmrow_dbtype_aligned))
        if ins_str_cols:
            ins_str = '''INSERT INTO %s (Created_datetime,Created_by,%s) VALUES (?,?,%s)''' % (
                table_name,ins_str_cols, ins_str_pars)
            # print(ins_str)
            try:
                conn.execute(ins_str, ls_ins_pars)
            except:
                raise

    if len(dfm_update_data) >0 and process_mode == 'w_update':
        # update logic: update the entry
        ls_upt_pars = []
        # rename the df with new cols name,use rename function with a dict of old column to new column mapping
        upt_cols_name = [x for x in dfm_update_data.columns if x not in key_cols]
        ls_colnames_dbupdate = list(map(special_process_col_name, upt_cols_name))
        upt_str_cols = '=?,'.join(ls_colnames_dbupdate) + '=?' + ", Last_modified_datetime = ?,Last_modified_by=?"
        upt_str_keycols = '=? AND '.join(key_cols) + '=? AND Trans_Datetime = ?'

        for index,row in dfm_update_data.iterrows():
            logprint('Update table %s Record %s at Period %s' % (table_name, [row[col] for col in key_cols], last_trading_day))
            upt_row = [row[x] for x in upt_cols_name]
            upt_keycols_dbtype_aligned = dbtype_aligned_format([row[col] for col in key_cols])
            ls_upt_pars.append(tuple(dbtype_aligned_format(upt_row)) + (timestamp, dict_misc_pars['update_by']) +
                               tuple(upt_keycols_dbtype_aligned)+(last_trading_daytime,))

        if ls_upt_pars:
            update_str = '''UPDATE %s SET %s 
                WHERE %s ''' % (table_name, upt_str_cols,upt_str_keycols)
            conn.execute(update_str, tuple(ls_upt_pars))

def load_dfm_to_db_single_value_by_mkt_stk_wo_datetime(market_id,item,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                               process_mode:str='w_check',float_fix_decimal = 4):
    """
    针对单个market和stock组合,char为单值且与时间无关的记录处理,本函数只处理没有时间相关的主键的表.比如股票的发行数据,是一次性数据,后续不可能再
    有新的记录.
    1) 如果
    :param market_id:
    :param item:
    :param dfm_data: 只需包含当前获得的具体char值
    :param table_name:
    :param dict_misc_pars:
    :param process_mode:
    :return:
    """

    dt_key_cols = {'Market_ID': market_id, 'Stock_ID': item}
    load_dfm_to_db_single_value_by_key_cols_wo_datetime(dt_key_cols, dfm_data, table_name, dict_misc_pars, process_mode,
                                               float_fix_decimal)

def load_dfm_to_db_single_value_by_key_cols_wo_datetime(dt_key_cols:dict,dfm_data:DataFrame,table_name:str,dict_misc_pars:dict,
                                              process_mode,float_fix_decimal):
    """
    本函数用于单值属性的char的当前数据更新.
    1) 如果处理模式为'w_check',则会检dfm中的数据是否和最近的DB记录是否一致,不一致才进行更新,
    2) 非以上情况, 直接报错
    :param dt_key_cols: 注意,所有DB表中的key cols的值必须都要提供,否则程序会出现不可预知的错误!
    :param dfm_data:
    :param table_name:
    :param dict_misc_pars:
    :return:
    """
    if process_mode == 'w_check':
        # load DB contents
        timestamp = datetime.now()

        ls_key_col_items = dt_key_cols.items()
        key_cols =[x[0] for x in ls_key_col_items]
        ls_sel_key_cols = [ x[0] + ' = ?' for x in ls_key_col_items]   # ['Market_ID = ?','Stock_ID = ?']
        ls_key_cols_value = [x[1] for x in ls_key_col_items]       # ['SH','600000']

        str_key_cols = ','.join([x[0] for x in ls_key_col_items])
        mark_key_cols = ','.join(['?'] * len(dt_key_cols))
        str_sel_key_cols = ' and '.join(ls_sel_key_cols)

        last_trading_day = gcf.get_last_trading_day()
        last_trading_daytime = datetime.strptime(str(last_trading_day), '%Y-%m-%d')

        dfm_db_data = pd.read_sql_query("select * from %s where %s " %(table_name,str_sel_key_cols)
                                            , conn, params=tuple(ls_key_cols_value))
        # print(dfm_data)
        # print(dfm_db_data)

        db_flg = 'NA'
        if len(dfm_db_data) > 0:
            # only insert DB in case the value is different from latest datetime's values
            s_db_data = dfm_db_data.iloc[0]
            for col in dfm_data.columns:
                # dfm_data的列名有可能带[],但是dataframe从sql server中读出时的列名是都不带[],所以要把[]去掉,再进行数据比较.
                tmp_colname = col.replace('[', '').replace(']', '')
                if tmp_colname in dfm_db_data.columns:
                    # 如果两种是空值,只是None的类型不同,不处理.
                    if pd.isnull(dfm_data.iloc[0][col]) and pd.isnull(s_db_data[tmp_colname]):
                        continue
                    if dfm_data.iloc[0][col] != s_db_data[tmp_colname]:
                        # if numpy.float64 then it is possible the value has slight difference,so change rule to allow dif <0.0001
                        dif = 1. / 10 ** float_fix_decimal
                        try:
                            dif = abs(dfm_data.iloc[0][col] - s_db_data[tmp_colname])
                        except:
                            dif = 1.
                        if isinstance(dif, Timedelta) or dif >= 1. / 10 ** float_fix_decimal:
                            # cur data is different from db data of latest transaction datetime
                            db_flg = 'UPDATE'
                            break
                else:
                    assert 0==1,"Column name %s doesn't exist in table %s but in dataframe" % (tmp_colname, table_name)
        else:
            # no db entry
            db_flg = 'INSERT'
        if db_flg == 'INSERT':
            # insert logic
            logprint('Insert %s %s' % (table_name, dt_key_cols))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbinsert = list(map(special_process_col_name, dfm_data.columns))
            ins_str_cols = ','.join(ls_colnames_dbinsert)
            ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
            ls_dfmrow_dbtype_aligned = dbtype_aligned_format(dfm_data.iloc[0])
            ls_ins_pars = []
            ls_ins_pars.append((*ls_key_cols_value, timestamp, dict_misc_pars['update_by'])
                               + tuple(ls_dfmrow_dbtype_aligned))
            ins_str = '''INSERT INTO %s (%s,Created_datetime,Created_by,%s) VALUES (%s,?,?,%s)''' % (
                table_name, str_key_cols, ins_str_cols, mark_key_cols, ins_str_pars)
            # print(ins_str)
            try:
                for ins_par in ls_ins_pars:
                    conn.execute(ins_str, ins_par)
            except:
                raise
        elif db_flg == 'UPDATE':
            logprint('UPDATE %s %s' % (table_name, dt_key_cols))

            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbupdate = list(map(special_process_col_name,dfm_data.columns))
            upt_str_cols = '=?,'.join(ls_colnames_dbupdate) +'=?'+ ", Last_modified_datetime = ?,Last_modified_by=?"
            upt_str_keycols = '=? AND '.join(key_cols) +'=?'

            ls_upt_pars = []
            ls_upt_pars.append(tuple(dbtype_aligned_format(dfm_data.iloc[0])) + (timestamp, dict_misc_pars['update_by']) +
                               tuple(ls_key_cols_value))

            update_str = '''UPDATE %s SET %s 
                WHERE %s ''' % (table_name, upt_str_cols,upt_str_keycols)
            try:
                conn.execute(update_str, tuple(ls_upt_pars))
            except:
                raise


    else:
        assert 0==1,'Unkown process_mode, exception raised!'


def load_snapshot_dfm_to_db(dfm_log:DataFrame,table_name,mode:str = '', w_timestamp:bool = False ):
    """
    currently support 2 mode:
    1. used for YY_tables as log table update, in this case, w_timestamp = False, func will add a new column Update_time
    2. used as general insert entries into table(no update func, just insert), in this case, set w_timestamp = True
    caution: dfm_log must have same columns as db table.
    :param dfm_log: the dataframe to insert into db
    :param table_name:  the db table name
    :param mode: 'del&recreate' ->del all table entries first; ''-> only insert
    :param w_timestamp: True -> don't add Update_time col; False-> add Update_time col
    :return:
    """
    if not w_timestamp:
        dfm_log['Update_time'] = datetime.now()

    if mode == 'del&recreate':
        conn.execute('DELETE FROM %s' %table_name)

    ls_colnames_dbinsert = list(map(special_process_col_name, dfm_log.columns))
    ins_str_cols = ','.join(ls_colnames_dbinsert)
    ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
    ls_ins_pars = []
    for i in range(len(dfm_log)):
        ls_dfmrow_dbtype_aligned = dbtype_aligned_format(dfm_log.iloc[i])
        ls_ins_pars.append(ls_dfmrow_dbtype_aligned)
    ins_str = "INSERT INTO %s (%s) VALUES (%s)" %(table_name,ins_str_cols,ins_str_pars)
    for ins_par in ls_ins_pars:
        conn.execute(ins_str,ins_par)


def special_process_col_name(tempstr:str):
    """
    due to col name in db has some restriction, need to reprocess dfm columns name to make it align with DB's column name
    SQL server2008里面如果列名里有特殊字符,则会自动在列名前后加上[],如果没有特殊字符,则列名不变;在查询,修改时,如果列名是带方括号的,则必须
    用[]的列名,如果没有带[]的,则列名用[]或不用[]都是可以的.
    此外,同dataframe从sql server2008读出的列名里都是不带[]的.
    所以这里的特殊处理就是
    such as add[]
    :param tempstr:
    :return:
    """
    tempstr = tempstr.replace("[","")
    tempstr = tempstr.replace("]","")
    tempstr = '['+ tempstr+ ']'
    return tempstr

def get_catg(origin:str) -> DataFrame:
    """
    获得股票的分类信息
    :return: dataframe of category
    """
    if origin == "":
        dfm_catg = pd.read_sql_query('''select * from ZCFG_category '''
                                   , conn)
       # print(dfm_stocks)
    else:
        dfm_catg = pd.read_sql_query('''select * from ZCFG_category
                                                where Catg_Origin = '%s' ''' %origin
                                       , conn)
    return dfm_catg

def get_last_fetch_date(program_name:str, format ='datetime'):
    """
    get last fetch date from table_name, use to update periodic date considering interval as parameter
    such as stock dailybar
    :param table_name:
    :return:
    """
    dfm_last_fetch_day = pd.read_sql_query("select last_fetch_date from DZ_scrap_job_last_fetch_date where program_name = '%s'" %program_name
                                   , conn)

    if len(dfm_last_fetch_day) == 0:
        return None
    elif format == 'detetime':
        return dfm_last_fetch_day.iloc[0]['last_fetch_date']
    elif format == 'date':
        return dfm_last_fetch_day.iloc[0]['last_fetch_date'].date()
    else:
        return dfm_last_fetch_day.iloc[0]['last_fetch_date'].strftime(format)





# SQL server don't support ALTER AFTER ,no no way to change the columns sequence.
# def get_col_for_insert(table_name, col_insert_before:str ='Created_datetime'):
#     """
#     there are 4 fours currently in table template for DD_tables defination, but I want to insert new columns before these
#     4 common cols, so I use sys tabel to get the last col name before these 4 standard template cols.
#     Since SQL only support "insert after", no "insert before"
#     :param table_name:
#     :param col_insert_before:
#     :return:
#     """
#     dfm_db1 = pd.read_sql_query(
#         "select * from SysColumns Where ID = OBJECT_ID('%s')" %table_name
#         , conn)
#
#     #print(dfm_db1)
#     insert_offset = dfm_db1[dfm_db1.name == col_insert_before].index[0] -1
#
#     return dfm_db1.iloc[insert_offset]['name']

def updateDB_last_fetch_date(program_name,fetch_datetime):
    dfm1 = DataFrame([{'program_name':program_name,'last_fetch_date':fetch_datetime}])
    print(dfm1)
    key_cols = ['program_name']
    dfm_to_db_insert_or_update(dfm1,key_cols,'DZ_scrap_job_last_fetch_date',program_name)

def dfm_to_db_insert_or_update(dfm_cur_data: DataFrame, key_cols: list, table_name: str, updated_by:str, process_mode:str='w_update'):
    """
    本函数用于通用的dfm的数据直接更新table,传入的key_cols list必须包含table中的所有key cols,.
    导入的dfm中的数据到table中,processing_mode决定了处理方式:
    1) 'w_update: key_cols存在时, 进行update
    2) "wo_update: key_cols存在时,不进行update
    :param dfm:
    :param table_name:
    :param dict_misc_pars:
    :processing_mode:
    :return:
    """

    # load DB contents
    timestamp = datetime.now()
    dfm_db_data = pd.read_sql_query("select * from %s " % table_name, conn)
    dfm_insert_data = gcf.dfm_A_minus_B(dfm_cur_data, dfm_db_data, key_cols)
    dfm_update_data = gcf.dfm_A_intersect_B(dfm_cur_data, dfm_db_data, key_cols)

    # gcf.dfmprint(dfm_insert_data)
    if len(dfm_insert_data) > 0:
        ins_str_cols = ''
        ins_str_pars = ''
        ls_ins_pars = []
        for index, row in dfm_insert_data.iterrows():
            # insert logic
            logprint('Insert table %s new Record %s' % (
            table_name, [row[col] for col in key_cols]))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbinsert = list(map(special_process_col_name, dfm_insert_data.columns))
            ins_str_cols = ','.join(ls_colnames_dbinsert)
            ins_str_pars = ','.join('?' * len(ls_colnames_dbinsert))
            #        print(ins_str_cols)
            #        print(ins_str_pars)
            # convert into datetime type so that it can update into SQL server
            # trans_datetime = datetime.strptime(str(ts_id.date()), '%Y-%m-%d')
            # trans_datetime = ts_id.to_pydatetime()
            ls_dfmrow_dbtype_aligned = dbtype_aligned_format(row)
            ls_ins_pars.append((timestamp, updated_by)
                               + tuple(ls_dfmrow_dbtype_aligned))
        if ins_str_cols:
            ins_str = '''INSERT INTO %s (Created_datetime,Created_by,%s) VALUES (?,?,%s)''' % (
                table_name, ins_str_cols, ins_str_pars)
            # print(ins_str)
            try:
                for ins_par in ls_ins_pars:
                    conn.execute(ins_str, ins_par)
            except:
                raise

    if len(dfm_update_data) > 0 and process_mode == 'w_update':
        # update logic: update the entry
        ls_upt_pars = []
        for index, row in dfm_update_data.iterrows():
            logprint('Update table %s Record %s' % (
            table_name, [row[col] for col in key_cols]))
            # rename the df with new cols name,use rename function with a dict of old column to new column mapping
            ls_colnames_dbupdate = list(map(special_process_col_name, dfm_update_data.columns))
            upt_str_cols = '=?,'.join(
                ls_colnames_dbupdate) + '=?' + ", Last_modified_datetime = ?,Last_modified_by=?"
            upt_str_keycols = '=? AND '.join(key_cols) + '=?'
            ls_dfmrow_dbtype_aligned = dbtype_aligned_format([row[col] for col in key_cols])
            ls_upt_pars.append(tuple(dbtype_aligned_format(row)) + (timestamp, updated_by) +
                               tuple(ls_dfmrow_dbtype_aligned))

        if ls_upt_pars:
            update_str = '''UPDATE %s SET %s 
                WHERE %s ''' % (table_name, upt_str_cols, upt_str_keycols)
            conn.execute(update_str, tuple(ls_upt_pars))

    # time.sleep(0)


if __name__ == "__main__":
    # print(get_cn_stocklist())
    # print(get_chars('Tquant',['FIN10','FIN20','FIN30']))
    # create_table_by_stock_date_template('hello123')
    # dict_misc_pars = {}
    # dict_misc_pars['char_origin'] = 'Tquant'
    # dict_misc_pars['char_freq'] = "D"
    # dict_misc_pars['allow_multiple'] ='N'
    # dict_misc_pars['created_by'] = 'fetch_stock_3fin_report_from_tquant'
    # dict_misc_pars['char_usage'] = 'FIN10'
    # add_new_chars_and_cols({'test1':'decimal(18,2)','test2':'decimal(18,2)'},[],'stock_fin_balance_1',dict_misc_pars)
#    conn.execute('INSERT INTO YY_stock_changes_qq ([公布前内容],[公布后内容],[公布日期],[变动日期],[变动项目],[Stock_ID],
    # [Market_ID],[Update_time]) VALUES (?,?,?,?,?,?,?,?)', ('北京中天信会计师事务所', '四川华信(集团)会计师事务所',
    # '2002-01-12 00:00:00', None, '境内会计师事务所', '000155', 'SZ', '2017-09-12 21:13:12'))

    # dfmtest = get_cn_stocklist('',ls_excluded_stockids=['000001','600000','000002'])
    # gcf.dfmprint(dfmtest)
    # pass

    # print(exec_store_procedure(conn,'CFM_get_lastest_snapshot_from_mtkstk_hist_table',{'table_name': 'DD_stock_name_change_hist_qq'}))
    gcf.dfmprint(get_lastest_snapshot_from_mtkstk_hist_table('DD_stock_name_change_hist_Tquant'))
