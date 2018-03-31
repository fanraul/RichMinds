import pandas as pd
from pandas import Series, DataFrame
import numpy as np
import urllib.error
from bs4 import BeautifulSoup
import re
from datetime import datetime

import time
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf
from R50_general.general_helper_funcs import logprint, get_tmp_file
import R50_general.dfm_to_table_common as df2db
import xlrd

global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

excel_path = R50_general.general_constants.emchoice_excel_output_path

bulkinsert_script_file = r'C:\Users\Terry Fan\Desktop\bulkinsert_script.data'

ls_pars = [
    'PRECLOSE',  # 前收盘价
    'OPEN',  # 开盘价
    'HIGH',  # 最高价
    'LOW',  # 最低价
    'CLOSE',  # 收盘价
    'CHANGE',  # 涨跌
    'PCTCHANGE',  # 涨跌幅
    'VOLUME',  # 成交量
    'AMOUNT',  # 成交额
    'AVERAGE',  # 均价
    'TURN',  # 换手率
    'AMPLITUDE',  # 振幅
    'TRADESTATUS',  # 交易状态
    'TNUM',  # 成交笔数
    'TAFACTOR',  # 复权因子(后)
    'BUYVOL',  # 内盘成交量
    'SELLVOL',  # 外盘成交量
    'HIGHLIMIT',  # 是否涨停
    'LOWLIMIT',  # 是否跌停
    'ISSTSTOCK',  # 是否ST
    'ISXSTSTOCK',  # 是否*ST
]

dict_pars = {
    '时间': 'Trans_Datetime',
    '前收盘价':'PRECLOSE',  # 前收盘价
    '开盘价':'OPEN',  # 开盘价
    '最高价':'HIGH',  # 最高价
    '最低价':'LOW',  # 最低价
    '收盘价':'CLOSE',  # 收盘价
    '涨跌':'CHANGE',  # 涨跌
    '涨跌幅':'PCTCHANGE',  # 涨跌幅
    '成交量':'VOLUME',  # 成交量
    '成交额':'AMOUNT',  # 成交额
    '均价':'AVERAGE',  # 均价
    '换手率':'TURN',  # 换手率
    '振幅':'AMPLITUDE',  # 振幅
    '交易状态':'TRADESTATUS',  # 交易状态
    '成交笔数':'TNUM',  # 成交笔数
    '复权因子(后)':'TAFACTOR',  # 复权因子(后)
    '内盘成交量':'BUYVOL',  # 内盘成交量
    '外盘成交量':'SELLVOL',  # 外盘成交量
    '是否涨停':'HIGHLIMIT',  # 是否涨停
    '是否跌停':'LOWLIMIT',  # 是否跌停
    '是否ST':'ISSTSTOCK',  # 是否ST
    '是否*ST':'ISXSTSTOCK',  # 是否*ST
}

ls_filenames = []

def produce_bulkinsert_files():

    start_date = get_start_date()
    end_date = get_end_date()
    total_id = 351

    # init step
    # create DD tables for data store and add chars for stock structure.
    # get chars for name change hist
    dfm_db_chars = df2db.get_chars('EMCHOICE', ['DAILYBAR'])
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = 'EMCHOICE'
    dict_misc_pars['char_freq'] = "D"
    dict_misc_pars['allow_multiple'] = 'N'
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = global_module_name
    dict_misc_pars['char_usage'] = 'DAILYBAR'

    # check whether db table is created.
    table_name = R50_general.general_constants.dbtables['stock_dailybar_emchoice']
    df2db.create_table_by_template(table_name,table_type='stock_date')
    dict_cols_cur = {
                    'PRECLOSE':'decimal(12,4)',  # 前收盘价
                    'OPEN':'decimal(12,4)',  # 开盘价
                    'HIGH':'decimal(12,4)',  # 最高价
                    'LOW':'decimal(12,4)',  # 最低价
                    'CLOSE':'decimal(12,4)',  # 收盘价
                    'CHANGE':'decimal(12,4)',  # 涨跌
                    'PCTCHANGE':'decimal(12,4)',  # 涨跌幅
                    'VOLUME':'decimal(15,2)',  # 成交量
                    'AMOUNT':'decimal(15,2)',  # 成交额
                    'AVERAGE':'decimal(12,4)',  # 均价
                    'TURN':'decimal(12,4)',  # 换手率
                    'AMPLITUDE':'decimal(12,4)',  # 振幅
                    'TRADESTATUS':'nvarchar(50)',  # 交易状态
                    'TNUM':'decimal(15,2)',  # 成交笔数
                    'TAFACTOR':'decimal(10,6)',  # 复权因子(后)
                    'BUYVOL':'decimal(15,2)',  # 内盘成交量
                    'SELLVOL':'decimal(15,2)',  # 外盘成交量
                    'HIGHLIMIT':'nvarchar(1)',  # 是否涨停
                    'LOWLIMIT':'nvarchar(1)',  # 是否跌停
                    'ISSTSTOCK':'nvarchar(1)',  # 是否ST
                    'ISXSTSTOCK':'nvarchar(1)',  # 是否*ST
                    }
    df2db.add_new_chars_and_cols(dict_cols_cur, list(dfm_db_chars['Char_ID']), table_name, dict_misc_pars)

    ls_dfms =[]

    bis_handler = open(bulkinsert_script_file,'w',encoding='utf-8')

    for i in range(total_id):
        file_name = 'fetch_dailybars_%s_to_%s_%s.xlsx' %(start_date,end_date,str(i+1))
        filename_with_path = excel_path + file_name
        logprint('Processing file: %s ...' %filename_with_path)
        # load excel results into dfm

        # filename_with_path = r'C:\Users\Terry Fan\Desktop\fetch_dailybars_2005-01-01_to_2018-03-29_1.xlsx'
        dfm_stock_dailybars = pd.read_excel(filename_with_path,sheetname='hist',header=1,skip_footer=2)

        dfm_fmt_dailybar = format_dfm(dfm_stock_dailybars)

        for index,row in dfm_fmt_dailybar.iterrows():
            if row['交易状态'] == '' or row ['交易状态'] == None:
                logprint('Error: file %s is incorrect, there are lines without 交易状态!' %file_name)
            bis_handler.write(';'.join([str(x) if str(x) != 'nan' else '' for x in row]))
            bis_handler.write('\n')

        ls_dfms.append(dfm_fmt_dailybar)

    bis_handler.close()
    dfm_fmt_dailybar.to_csv(get_tmp_file('tmp2.csv'))

def format_dfm(dfm_dailybar:DataFrame):

    # 将股票代码拆成市场和股票代码两列
    dfm_dailybar = pd.concat([dfm_dailybar, dfm_dailybar['代码'].str.split('.', expand=True)], axis=1)
    dfm_dailybar.rename(columns={0: 'Stock_ID', 1: 'Market_ID'}, inplace=True)

    dfm_dailybar['是否涨停']= dfm_dailybar['是否涨停'].map({'是': 'Y','否':'N'})
    dfm_dailybar['是否跌停']= dfm_dailybar['是否跌停'].map({'是': 'Y','否':'N'})
    dfm_dailybar['是否ST']= dfm_dailybar['是否ST'].map({'是': 'Y','否':'N'})
    dfm_dailybar['是否*ST']= dfm_dailybar['是否*ST'].map({'是': 'Y','否':'N'})
    dfm_dailybar = dfm_dailybar[dfm_dailybar['交易状态'] != '未上市']
    dfm_dailybar['created_by'] = global_module_name
    dfm_dailybar['created_datetime'] = datetime.now()
    dfm_dailybar['updated_by'] = ''
    dfm_dailybar['updated_datetime'] = ''
    return dfm_dailybar[['Market_ID',
                         'Stock_ID',
                         '时间',
                         'created_datetime',
                         'created_by',
                         'updated_datetime',
                         'updated_by',
                        '前收盘价',  # 前收盘价
                        '开盘价',  #:'OPEN',  # 开盘价
                        '最高价',  #:'HIGH',  # 最高价
                        '最低价',  #:'LOW',  # 最低价
                        '收盘价',  #:'CLOSE',  # 收盘价
                        '涨跌',  #'CHANGE',  # 涨跌
                        '涨跌幅',  #'PCTCHANGE',  # 涨跌幅
                        '成交量',  #'VOLUME',  # 成交量
                        '成交额',  #'AMOUNT',  # 成交额
                        '均价',  #'AVERAGE',  # 均价
                        '换手率',  #'TURN',  # 换手率
                        '振幅',  #'AMPLITUDE',  # 振幅
                        '交易状态',  #'TRADESTATUS',  # 交易状态
                        '成交笔数',  #'TNUM',  # 成交笔数
                        '复权因子(后)',  #'TAFACTOR',  # 复权因子(后)
                        '内盘成交量',  #'BUYVOL',  # 内盘成交量
                        '外盘成交量',  #'SELLVOL',  # 外盘成交量
                        '是否涨停',  #'HIGHLIMIT',  # 是否涨停
                        '是否跌停',  #'LOWLIMIT',  # 是否跌停
                        '是否ST',  #'ISSTSTOCK',  # 是否ST
                        '是否*ST',  #'ISXSTSTOCK',  # 是否*ST
           ]]

def get_start_date():
    return '2005-01-01'

def get_end_date():
    return '2018-03-29'

if __name__ == '__main__':
    produce_bulkinsert_files()


'''
bulk insert file format:
<?xml version="1.0"?>

-<BCPFORMAT xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://schemas.microsoft.com/sqlserver/2004/bulkload/format">

-<ROW>

<COLUMN xsi:type="SQLNVARCHAR" NAME="Market_ID" SOURCE="1"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="Stock_ID" SOURCE="2"/>

<COLUMN xsi:type="SQLDATETIME" NAME="Trans_Datetime" SOURCE="3"/>

<COLUMN xsi:type="SQLDATETIME" NAME="Created_datetime" SOURCE="4"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="Created_by" SOURCE="5"/>

<COLUMN xsi:type="SQLDATETIME" NAME="Last_modified_datetime" SOURCE="6"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="Last_modified_by" SOURCE="7"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="PRECLOSE" SOURCE="8" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="OPEN" SOURCE="9" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="HIGH" SOURCE="10" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="LOW" SOURCE="11" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="CLOSE" SOURCE="12" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="CHANGE" SOURCE="13" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="PCTCHANGE" SOURCE="14" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="VOLUME" SOURCE="15" SCALE="2" PRECISION="15"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="AMOUNT" SOURCE="16" SCALE="2" PRECISION="15"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="AVERAGE" SOURCE="17" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="TURN" SOURCE="18" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="AMPLITUDE" SOURCE="19" SCALE="4" PRECISION="12"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="TRADESTATUS" SOURCE="20"/>

<COLUMN xsi:type="SQLINT" NAME="TNUM" SOURCE="21"/>

<COLUMN xsi:type="SQLDECIMAL" NAME="TAFACTOR" SOURCE="22" SCALE="6" PRECISION="10"/>

<COLUMN xsi:type="SQLINT" NAME="BUYVOL" SOURCE="23"/>

<COLUMN xsi:type="SQLINT" NAME="SELLVOL" SOURCE="24"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="HIGHLIMIT" SOURCE="25"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="LOWLIMIT" SOURCE="26"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="ISSTSTOCK" SOURCE="27"/>

<COLUMN xsi:type="SQLNVARCHAR" NAME="ISXSTSTOCK" SOURCE="28"/>

</ROW>

-<RECORD>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="100" TERMINATOR=";" xsi:type="CharTerm" ID="1"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="100" TERMINATOR=";" xsi:type="CharTerm" ID="2"/>

<FIELD MAX_LENGTH="24" TERMINATOR=";" xsi:type="CharTerm" ID="3"/>

<FIELD MAX_LENGTH="24" TERMINATOR=";" xsi:type="CharTerm" ID="4"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="100" TERMINATOR=";" xsi:type="CharTerm" ID="5"/>

<FIELD MAX_LENGTH="24" TERMINATOR=";" xsi:type="CharTerm" ID="6"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="100" TERMINATOR=";" xsi:type="CharTerm" ID="7"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="8"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="9"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="10"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="11"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="12"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="13"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="14"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="15"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="16"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="17"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="18"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="19"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="100" TERMINATOR=";" xsi:type="CharTerm" ID="20"/>

<FIELD MAX_LENGTH="12" TERMINATOR=";" xsi:type="CharTerm" ID="21"/>

<FIELD MAX_LENGTH="41" TERMINATOR=";" xsi:type="CharTerm" ID="22"/>

<FIELD MAX_LENGTH="12" TERMINATOR=";" xsi:type="CharTerm" ID="23"/>

<FIELD MAX_LENGTH="12" TERMINATOR=";" xsi:type="CharTerm" ID="24"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="2" TERMINATOR=";" xsi:type="CharTerm" ID="25"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="2" TERMINATOR=";" xsi:type="CharTerm" ID="26"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="2" TERMINATOR=";" xsi:type="CharTerm" ID="27"/>

<FIELD COLLATION="Chinese_PRC_CI_AS" MAX_LENGTH="2" TERMINATOR="\r\n" xsi:type="CharTerm" ID="28"/>

</RECORD>

</BCPFORMAT>
'''