"""
load data from excel,and generate insert sql statement and export to excel
"""

import pandas as pd
from pandas import DataFrame,Series
import R50_general.general_helper_funcs as gcf

excel_path = 'C:\\00_RichMinds\\excels\\'

# table name for insert
upt_table_name = 'DD_stocklist_hkus_futuquant'

# excel name which contains the upload data
excel_name = 'idx which donot have stock assigned.xlsx'

dfm_excel = pd.read_excel(excel_path+excel_name)

ls_cols = list(dfm_excel.columns)

sep_idx = ls_cols.index('key_value_seperator')
ls_key_cols = ls_cols[:sep_idx]
ls_upt_cols = ls_cols[sep_idx+1:]

ls_dfm_upt_sql = []

for index,row in DataFrame.iterrows(dfm_excel):
    ls_key_values = list(row)[:sep_idx]
    ls_upt_values = list(row)[sep_idx+1:]
    dt_key = dict(zip(ls_key_cols,ls_key_values))
    dt_upt = dict(zip(ls_upt_cols,ls_upt_values))

    str_set_values = ','.join(["[%s] = '%s'" %(key,value) for (key,value) in dt_upt.items()])
    str_where_keys = ' AND '.join(["[%s] = '%s'" %(key,value) for (key,value) in dt_key.items()])

    str_upt_sql = 'UPDATE %s SET %s WHERE %s' %(upt_table_name, str_set_values, str_where_keys)
    ls_dfm_upt_sql.append({'UPDATE_SQL': str_upt_sql})

dfm_upt_sql = DataFrame(ls_dfm_upt_sql)
gcf.dfmprint(dfm_upt_sql)

dfm_upt_sql.to_excel(excel_path+excel_name.split('.')[0]+'_converted.xlsx')
