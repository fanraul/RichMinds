# Readme
## Program overview

 程序名 | 数据对象 | 数据源 | DB更新函数 | 其他
 ---   |---      |---    |---        |---
fetch_stocklist_hkus_from_futuquant | Stocklist_of_CN/HK/US:DD_stocklist_hkus_futuquant(key:market/stock/listing date)  | Futuquant_api |General DB update func:dfm_to_db_insert_or_update | |
fetch_stock_index_stocks_from_futuquant|Stocks_of_index_for_CN/HK/US:DD_stock_index_stocks_futuquant<br>(key:market/stock/valid_from/multi) |Futuquant_api|Mtkstk multi current(last_trading_date) func:load_dfm_to_db_multi_value_by_mkt_stk_cur
fetch_stock_news_cn_from_jd|



## Template program for different scenarios
### stock id + transdate as key
+ Current data, web site, single value -> DB: fetch_stock_company_general_info_from_eastmoney
+ Current data, web site, multiple value -> DB:
+ History data, web site, single value  -> DB: 
+ History data, web site, multiple value  -> DB: 
+ Current data, json, single value -> DB: fetch_stock_company_general_info_from_eastmoney
+ Current data, json, multiple value -> DB:
+ History data, json, single value  -> DB: 
+ History data, json, multiple value  -> DB: 
+ Current data, api, single value -> DB: fetch_stock_current_dailybar_from_sina
+ Current data, api, multiple value -> DB: 
+ History data, api, single value  -> DB: fetch_stock_dailybar_from_tquant
+ History data, api, multiple value  -> DB: 

### stock id  as key
### obj other than stock id + transdate as key
### obj other than stock id as key
