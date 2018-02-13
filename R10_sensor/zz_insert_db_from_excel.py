"""
load data from excel,and generate insert sql statement and export to excel
"""

import pandas as pd
from pandas import DataFrame,Series
import R50_general.general_helper_funcs as gcf

excel_path = 'C:\\00_RichMinds\\excels\\'

# table name for insert
insert_table_name = 'DD_stock_name_change_hist_Tquant'

# excel name which contains the upload data
excel_name = 'stock name changes during gap period.xlsx'

dfm_excel = pd.read_excel(excel_path+excel_name)

ls_cols = dfm_excel.columns

str_insert_colnames = ','.join(['['+ col +']' for col in ls_cols])

ls_dfm_insert_sql = []

for index,row in DataFrame.iterrows(dfm_excel):
    str_insert_values = ','.join(["'"+ str(value) +"'" for value in row])
    str_insert_sql = 'INSERT INTO %s (%s) VALUES (%s)' %(insert_table_name,str_insert_colnames,str_insert_values)
    ls_dfm_insert_sql.append({'INSERT_SQL': str_insert_sql})

dfm_insert_sql = DataFrame(ls_dfm_insert_sql)
gcf.dfmprint(dfm_insert_sql)

dfm_insert_sql.to_excel(excel_path+excel_name.split('.')[0]+'_converted.xlsx')
