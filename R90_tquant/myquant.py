# -*- coding: utf-8 -*-
import datetime
from gmsdk import md,to_dict
import pandas as pd
md.init('fanraul@21cn.com','tquant121')
CFFEX = ['IF','IH','IC','T','TF']
CZCE =['CF','FG','MA','RM','SR','TA','ZC']
SHFE = ['AL','BU','CU','HC','NI','RB','RU','SN','ZN']
DCE=['C','CS','I','J','JD','JM','L','M','P','PP','V','Y']
def mtsymbol_list(symbol_list):
    z = len (symbol_list)
    ret = ''
    for i in range(z):
        ret = ret + symbol_list[i] +','
    ret = ret[:len(ret)-1]
    return ret
def to_pd(var,index):
    ret =[]
    for i in var:
        ret.append(to_dict(i))
    ret = pd.DataFrame (ret)
    ret = ret.set_index(index)
    return ret
def get_shse( ):
    var =md.get_instruments('SHSE', 1, 0)
    return to_pd(var,'symbol')
def get_szse():
    var =md.get_instruments('SZSE', 1, 0)
    return to_pd(var,'symbol')
def get_shfe():
    var = md.get_instruments('SHFE', 4, 1)
    return to_pd(var,'symbol')
def get_dce():
    var = md.get_instruments('DCE', 4, 1)
    return to_pd(var,'symbol')
def get_czce():
    var =  md.get_instruments('CZCE', 4, 1)
    return to_pd(var,'symbol')
def get_cffex():
    var = md.get_instruments('CFFEX', 4, 1)
    return to_pd(var,'symbol')
def get_index():
    shse = md.get_instruments('SHSE', 3, 1)
    shse =to_pd(shse,'symbol')
    szse = md.get_instruments('SZSE', 3, 1)
    szse =to_pd(szse,'symbol')
    return shse.append(szse)
def get_etf():
     shse = md.get_instruments('SHSE', 5, 0)
     return to_pd(shse,'symbol')
def get_fund():
    shse = md.get_instruments('SHSE', 2, 0)
    shse =to_pd(shse,'symbol')
    szse = md.get_instruments('SZSE', 2, 0)
    szse =to_pd(szse,'symbol')
    return shse.append(szse)
def get_instruments_by_name(name):#期货接口
    var = md.get_instruments_by_name(name)
    z = len(var)
    for i in range (z):
        k = z-1-i
        if var[k].is_active == 0:
            del var[k]
    return to_pd(var,'symbol')
def get_constituents(index_symbol):#指数权重
    var = md.get_constituents(index_symbol)
    return to_pd(var,'symbol')

def get_financial_index(symbol, begin_datetime,end_datetime): # 基本财务指标
    t_begin = begin_datetime.strftime('%Y-%m-%d') +' 00:00:00'
    t_end   = end_datetime.strftime('%Y-%m-%d') + ' 15:00:00'
    var =md.get_financial_index(symbol, t_begin,t_end)
    var =to_pd(var,'pub_date')
    return var

def get_last_financial_index(symbol_list):
    var = md.get_last_financial_index(mtsymbol_list(symbol_list))
    var = to_pd(var,'symbol')
    return var

def get_share_index(symbol,begin_datetime,end_datetime):  #股本信息
    var = md.get_share_index(symbol,begin_datetime,end_datetime)
    var = to_pd(var, 'pub_date')
    return var

def get_latest_share_index(symbol_list):
    var = md.get_last_share_index(mtsymbol_list(symbol_list))
    var = to_pd(var, 'symbol')
    return var

def get_market_index(symbol,begin_datetime,end_datetime):  # 股票基本指标,市盈率,市净率etc.
    var = md.get_market_index(symbol,begin_datetime,end_datetime)
    var = to_pd(var, 'pub_date')
    return var

def get_latest_market_index(symbol_list):
    var = md.get_last_market_index(mtsymbol_list(symbol_list))
    var = to_pd(var, 'symbol')
    return var

def get_dividend(symbol,begin_datetime,end_datetime):  # 分红信息.
    var = md.get_divident(symbol,begin_datetime,end_datetime)
    var = to_pd(var, 'div_date')
    return var


def get_stock_adj(symbol,begin_datetime,end_datetime):
    var = md.get_stock_adj(symbol,begin_datetime,end_datetime)
    var = to_pd(var, 'trade_date')
    return var

def get_calendar(exchange, start_time, end_time):
    var = md.get_calendar(exchange, start_time, end_time)
    ret = []
    for i in var:
        Date = datetime.datetime.utcfromtimestamp(i.utc_time)
        ret.append (Date)
    return ret

####md
def tick_topd(var,index):
    ret = []
    for i in var:
        tmp = {}
        Date = datetime.datetime.utcfromtimestamp(i.utc_time)
        Date = Date + datetime.timedelta(hours=8)
        tmp['date'] = Date
        tmp['code'] = i.exchange + '.' + i.sec_id
        tmp['close'] = i.last_price
        tmp['vol'] = i.last_volume
        tmp['amount'] = i.last_amount
        tmp['opi'] = i.cum_position
        if len(i.bids) > 0:
            tmp['买一价'] = i.bids[0][0]
            tmp['买一量'] = i.bids[0][1]
        if len(i.asks) > 0:
            tmp['卖一价'] = i.asks[0][0]
            tmp['卖一量'] = i.asks[0][1]
        ret.append(tmp)
    ret = pd.DataFrame(ret)
    ret = ret.set_index(index)
    return ret
def get_ticks(symbol, begin_time, end_time):
    var = md.get_ticks(symbol, begin_time, end_time)
    if len(var) == 0:
        print("no ticks data for stock %s between %s and %s" %(symbol, begin_time, end_time))
        return pd.DataFrame()
    else:
        ret = tick_topd(var,'date')
        return ret
def bar_topd(var,index):
    ret = []
    z = len(var)
    for j in range (z):
        i =var[j]
        
        tmp = {}
        Date = datetime.datetime.utcfromtimestamp(i.utc_time)
        Date = Date + datetime.timedelta(hours=8)
        tmp['date'] = Date
        tmp['code'] = i.exchange + '.' + i.sec_id
        tmp['close'] = i.close
        tmp['high'] = i.high
        tmp['low'] = i.low
        tmp['open'] = i.open
        tmp['vol'] = i.volume
        tmp['amount'] = i.amount
        if i.exchange in ['SHSE','SZSE'] :
            tmp['adj'] = i.adj_factor
        else:
            tmp['opi'] = i.position
        ret.append(tmp)
    ret = pd.DataFrame(ret)
    ret = ret.set_index(index)
    return ret
def get_bars(symbol, bar_type, begin_time, end_time):
    var = md.get_bars(symbol, bar_type, begin_time, end_time)
    ret = bar_topd(var,'date')
    return ret
def get_dailybars(symbol, begin_time, end_time):
    var = md.get_dailybars(symbol, begin_time, end_time)
    ret = bar_topd(var,'date')
    return ret
def get_last_ticks(symbol_list):
    symbol_list = mtsymbol_list(symbol_list)
    var = md.get_last_ticks(symbol_list)
    ret = tick_topd(var,'code')
    return ret
def get_last_bars(symbol_list, bar_type):
    symbol_list = mtsymbol_list(symbol_list)
    var = md.get_last_bars(symbol_list, bar_type)
    ret = bar_topd(var,'code')
    return ret
def get_last_dailybars(symbol_list):
    symbol_list = mtsymbol_list(symbol_list)
    var = md.get_last_dailybars(symbol_list)
    ret = bar_topd(var,'code')
    return ret
def get_last_n_ticks(symbol, n):
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    var = md.get_last_n_ticks(symbol, n, end_time)
    ret = tick_topd(var,'date')
    return ret
def get_last_n_bars(symbol, bar_type, n):
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    VAR = md.get_last_n_bars(symbol, bar_type, n, end_time)
    z = len(VAR)
    var = []
    for i in range(z):
        var.append(VAR[z-1-i])
    ret = bar_topd(var,'date')
    return ret
def get_last_n_dailybars(symbol, n):
    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    VAR = md.get_last_n_dailybars(symbol, n, end_time)
    z = len(VAR)
    var = []
    for i in range(z):
        var.append(VAR[z-1-i])
    ret = bar_topd(var,'date')
    return ret


if __name__ == '__main__':  # sample test for demo
    import R50_general.general_helper_funcs as gcf
    import os
    # var = get_ticks("SHSE.600000", "2018-2-8 09:30:00",
    #                            "2018-2-8 15:00:00")
    # # var = get_shse()
    begin_date = datetime.datetime.strptime("2017-2-1","%Y-%m-%d")
    end_date = datetime.datetime.strptime("2018-3-3","%Y-%m-%d")

    symbol_list = ['SHSE.600100', 'SHSE.600000', 'SHSE.600030', 'SZSE.000002', 'SZSE.300124']
    symbol = 'SHSE.600000'
    n =10
    bar_type = 15*60 #15mins

    # var = get_last_ticks(symbol_list)
    # print('result of get_last_ticks' + '-'*60)
    # gcf.dfmprint(var)
    #
    # var = get_last_n_ticks(symbol, n)
    # print('result of get_last_n_ticks' + '-'*60)
    # gcf.dfmprint(var)
    #
    # var = get_last_bars(symbol_list, bar_type)
    # print('result of get_last_bars' + '-'*60)
    # gcf.dfmprint(var)
    #
    # var = get_last_n_bars(symbol, bar_type, n)
    # print('result of get_last_n_bars' + '-'*60)
    # gcf.dfmprint(var)
    #
    # var = get_last_dailybars(symbol_list)
    # print('result of get_last_dailybars' + '-'*60)
    # gcf.dfmprint(var)
    #
    # var = get_last_n_dailybars(symbol, n)
    # print('result of get_last_n_dailybars' + '-'*60)
    # gcf.dfmprint(var)
    #
    # # os._exit(0)
    #
    # #Tquant的dailybar
    # var = get_dailybars("SHSE.600547", begin_date, end_date)
    # print('result of get_dailybars' + '-'*60)
    # gcf.dfmprint(var)

    #Tquant的self-defined bar
    var = get_bars("SHSE.600000", 1*60, begin_date, end_date)
    print('result of get_bars' + '-'*60)
    # gcf.dfmprint(var)
    var.to_excel(gcf.get_tmp_file('600000_1min_myquant.xlsx'))
    # var = get_stock_adj('SZSE.300088',begin_date,end_date)
    # print('result of stock adjustment' + '-'*60)
    # gcf.dfmprint(var)
    #
    # begin_date1 = datetime.datetime.strptime("2012-3-4","%Y-%m-%d")
    # var = get_dividend('SHSE.600547',begin_date1,end_date)
    # print('result of divident' + '-'*60)
    # gcf.dfmprint(var)
    #
    # # os._exit(0)
    #
    # # 财务基本指标
    # begin_date1 = datetime.datetime.strptime("2017-3-4","%Y-%m-%d")
    # var = get_financial_index("SHSE.600000",begin_date1,end_date)
    # print('result of get_financial_index' + '-'*60)
    # print(var)
    #
    # # market index是每天的市盈率和市净率数据,建议集成到Tquant的stockdailybar中一次性写入dailybar的表中
    # var = get_market_index("SHSE.600000", begin_date, end_date)
    # print('result of get_market_index' + '-'*60)
    # gcf.dfmprint(var)
    #
    # # 指数中每个股票的权重,有用
    # var = get_constituents('SHSE.000001')
    # print('please check execl output for ' + '-'*60)
    # var.to_excel(gcf.get_tmp_file("constituents.xlsx"))
    #
    # # tick数据,暂时没有,建议先记录入数据库,待将来再用,建议存入一个独立的数据库
    # begin_date_tick = datetime.datetime.strptime("2018-2-1","%Y-%m-%d")
    # end_date_tick = datetime.datetime.strptime("2018-2-7","%Y-%m-%d")
    # var = get_ticks("SHSE.600000", begin_date_tick,end_date_tick)
    # print('please check execl output for get_ticks' + '-'*60)
    # var.to_excel(gcf.get_tmp_file("ticks.xlsx"))
    #
    # # 获取交易所的交易日历, return a list
    # now = datetime.datetime.now()
    # begin_date1 = now - datetime.timedelta(days=10)  # 当前日期-10天
    # end_date1 = now + datetime.timedelta(days=10)  # 当前日期+10天
    # var = get_calendar('SHSE', begin_date1,end_date1)
    # print('result of get_calendar' + '-'*60)
    # gcf.print_list_nice(var)

    #




