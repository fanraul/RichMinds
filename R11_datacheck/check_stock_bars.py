import pandas as pd
from pandas import Series, DataFrame
import numpy as np

from datetime import datetime

import R50_general.general_constants
from R50_general.general_helper_funcs import logprint,get_tmp_file
import R50_general.general_helper_funcs as gcf
import R50_general.dfm_to_table_common as df2db
# import R50_general.advanced_helper_funcs as ahf

from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port


global_module_name = gcf.get_cur_file_name_by_module_name(__name__)

check_period_starttime = datetime(2005,1,1,0)
check_period_endtime   = datetime(2018,3,31,23)


def check_bars(ls_tradingdates,table1,table2,dfm_stocks,dt_cols):
    sorted(ls_tradingdates)
    ls_checkresults =[]
    for index,row in dfm_stocks.iterrows():
        dfm_checkbar = check_bar(row['Market_ID'],row['Stock_ID'],ls_tradingdates,table1,table2,dt_cols)
        if len(dfm_checkbar) >0:
            ls_checkresults.append(dfm_checkbar)

    if ls_checkresults:
        return pd.concat(ls_checkresults)
    else:
        return DataFrame()


def check_bar(mtk_id,stk_id,ls_tradingdates,table1,table2,dt_cols):
    # use mix merge for both tables in one shot, so no need to use ls_tradingdates any more
    # TODO: consider 股票停牌/复牌信息进行数据校验,目前直接比较两个数据源,如果都不存在,则默认这天股票是停牌的
    # ls_tradingdatetimes = [datetime.strptime(x,'%Y-%m-%d') for x in ls_tradingdates]
    suffix1 = table1.split('_')[-1]
    suffix2 = table2.split('_')[-1]

    print('(%s vs %s) processing %s...' %(suffix1,suffix2,stk_id))
    start_time = ls_tradingdates[0]
    end_time = ls_tradingdates[-1]
    # get table data
    ls_cond = [{'db_col': 'Market_ID', 'db_oper': '=', 'db_val': "'%s'" % mtk_id},
               {'db_col': 'Stock_ID', 'db_oper': '=', 'db_val': "'%s'" % stk_id},
               {'db_col': 'Trans_Datetime', 'db_oper': '>=', 'db_val': "'%s'" % start_time},
               {'db_col': 'Trans_Datetime', 'db_oper': '<=', 'db_val': "'%s'" % end_time},
               {'db_col': 'vol', 'db_oper': '>', 'db_val': "0"},   # exclude 停牌日的数据，有的数据源包含停牌日，有的数据源不包含，顾用vol >0保证比较的都是有交易的日期。
               ]

    dfm_cond = DataFrame(ls_cond)
    dfm_table1 = df2db.get_data_from_DB(table1,dfm_cond)
    dfm_table1 = dfm_table1[['Market_ID','Stock_ID','Trans_Datetime'] + list(dt_cols.keys())]

    dfm_table2 = df2db.get_data_from_DB(table2,dfm_cond)
    dfm_table2 = dfm_table2[['Market_ID','Stock_ID','Trans_Datetime'] + list(dt_cols.keys())]


    if len(dfm_table1) == 0 and len(dfm_table2) == 0:
        return DataFrame()

    err_msg_stockid_no_result = ''
    if len(dfm_table1) == 0:
        err_msg_stockid_no_result = 'stockid %s doesnot exist in %s' %(stk_id,table1)

    if len(dfm_table2) == 0:
        err_msg_stockid_no_result = 'stockid %s doesnot exist in %s' %(stk_id,table2)


    dfm_check = pd.merge(dfm_table1,dfm_table2, how='outer',
                         on=['Market_ID','Stock_ID','Trans_Datetime'],
                         indicator= "Merge_type",
                         suffixes=('_%s' %(suffix1),'_%s' %(suffix2)))


    def compare_line(s):
        if err_msg_stockid_no_result:
            return err_msg_stockid_no_result

        if s['Merge_type'] == 'left_only':
            return 'entry in %s, not in %s' %(suffix1,suffix2)
        elif s['Merge_type'] == 'right_only':
            return 'entry not %s, in %s' %(suffix1,suffix2)

        str_diff = 'DIFF:'
        for col in dt_cols.keys():
            col1 = col+'_%s' %(suffix1)
            col2 = col+'_%s' %(suffix2)
            rule = dt_cols[col]
            is_equal,msg = check_col_rule(s[col1],s[col2],rule)
            if not is_equal:
                str_diff += '%s(%s);' %(col,msg)

        if str_diff == 'DIFF:':
            return 'SAME'
        else:
            return str_diff

    if len(dfm_check) == 0:
        return DataFrame()

    dfm_check['compare_result'] = dfm_check.apply(compare_line,axis =1)
    dfm_check = dfm_check[dfm_check['compare_result'] != 'SAME']

    return dfm_check

def check_col_rule(v1,v2,rule):
    if v1 == None:
        v1 =0
    if v2 == None:
        v2 =0

    rule_part1 = rule[0]
    rule_part2 = rule[1]

    if rule_part1 == 1:
        if round(v1,rule_part2) == round(v2,rule_part2):
            return True,None
        else:
            return False, '%s <> %s at decimal %s' %(v1,v2,rule_part2)

    if rule_part1 == 3:
        if abs(v1-v2) <= rule_part2:
            return True,None
        else:
            return False, 'the abs diff between %s and %s > %s' %(v1,v2,rule_part2)

    assert 0==1,'unknown rule type!'


def check_cn_dailybars(stockid):
    # ls_trading_dates = gcf.get_trading_days_futuquant('SH',
    #                                                   check_period_starttime.strftime('%Y-%m-%d'),
    #                                                   check_period_endtime.strftime('%Y-%m-%d'))

    ls_trading_dates = [check_period_starttime.strftime('%Y-%m-%d'),check_period_endtime.strftime('%Y-%m-%d')]
    dfm_stocks = df2db.get_cn_stocklist(stockid)

    #compare two tables at one time
    tquant_bars = 'DD_stock_dailybar_Tquant'
    netease_bars = 'DD_stock_dailybar_netease'
    futu_bars = 'DD_stock_dailybar_futuquant'
    emchoice_bars = 'DD_stock_dailybar_emchoice'

    # netease vs emchoice
    dt_cols_netease_emchoice = {'open': (1,2),    #(1,2) means float compare equal in 2 decimal
                              'close':(1,2),
                              'low':(1,2),
                              'high':(1,2),
                              'vol':(3,10000), # (3,10000) means float compare, the absolute difference bewteen two value should less than or equal to 10000
                              'amount':(3,100000),
                              'preclose':(1,2),
                              # 'turnover': (3,0.01),  # 换手率

                               }
    dfm_checkresults = check_bars(ls_trading_dates,netease_bars,emchoice_bars,dfm_stocks,dt_cols_netease_emchoice)

    dfm_compare_results_to_file(dfm_checkresults,'netease','emchoice',stockid)

    # # Tquant vs netease
    dt_cols_tquant_netease = {'open': (1,2),    #(1,2) means float compare equal in 2 decimal
                              'close':(1,2),
                              'low':(1,2),
                              'high':(1,2),
                              'vol':(3,10000), # (3,10000) means float compare, the absolute difference bewteen two value should less than or equal to 10000
                              'amount':(3,100000),
                               }
    # dfm_checkresults = check_bars(ls_trading_dates,tquant_bars,netease_bars,dfm_stocks,dt_cols_tquant_netease)
    #
    # dfm_compare_results_to_file(dfm_checkresults,'tquant','netease',stockid)

    # Tquant vs emchoice
    dt_cols_tquant_emchoice = {
                              'open': (1,2),    #(1,2) means float compare equal in 2 decimal
                              'close':(1,2),
                              'low':(1,2),
                              'high':(1,2),
                              'vol':(3,10000),
                              'amount':(3,100000),
                             }
    dfm_checkresults = check_bars(ls_trading_dates,tquant_bars,emchoice_bars,dfm_stocks,dt_cols_tquant_emchoice)

    dfm_compare_results_to_file(dfm_checkresults,'tquant','emchoice',stockid)

    # futuquant vs emchoice
    # dt_cols_futuquant_emchoice = {'open': (1,2),    #(1,2) means float compare equal in 2 decimal
    #                           'close':(1,2),
    #                           'low':(1,2),
    #                           'high':(1,2),
    #                           'vol':(3,10000),
    #                           'amount':(3,100000),
    #                             }
    # dfm_checkresults = check_bars(ls_trading_dates,futu_bars,emchoice_bars,dfm_stocks,dt_cols_futuquant_emchoice)
    #
    # dfm_compare_results_to_file(dfm_checkresults,'futuquant','emchoice',stockid)

def dfm_compare_results_to_file(dfm_checkresults,sc1,sc2,stockid):

    now = datetime.now().strftime('%m%d-%H_%M_%S')

    if len(dfm_checkresults) > 0:
        dfm_checkresults.to_excel(get_tmp_file('%s_%s_%s_vs_%s_dailybars_check_result.xlsx' %(now,stockid,sc1,sc2)))
    else:
        logprint('There is no difference for (%s)_%s_vs_%s_dailybars'%(stockid,sc1,sc2))



if __name__ == '__main__':
    # check_cn_dailybars('600000')
    #check_cn_dailybars('6%')
    #check_cn_dailybars('0%')
    # check_cn_dailybars('9%')
    # check_cn_dailybars('2%')
    check_cn_dailybars('3%')

