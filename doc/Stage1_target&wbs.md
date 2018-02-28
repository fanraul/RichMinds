# 1.Stage1 Target
Build basic analysis functions in CN stock market based on:
+ stock K line data
+ stock volume data
+ stock holder data
+ category/index data
+ stock basic info
+ stock dividend info
+ stock fin reports
+ stock structure hist

# 2.Stage1 plan
add one month for buffer, total go-live date: 2018/7/1
+ 1 month for finalize all required data source (3.15)
+ 1 week for stock overview web page (3.22)
+ 1 week for buy and bell evaluator(4.1)
+ 2 months for all kinds of event evaluator(6.1)

## 2.1 WBS for stage1
+ CN market data source to be finalized
  - [x] stock list & basic info
      - 上海及深圳上市的股票清单now: BD_L2_00_cn_stocklist_with_general_info_now
      - 上海及深圳股票历史名称变更:BD_L1_10_cn_stock_name_changes_hist
  - [ ] stock dailybar
      - tick info
      - daily bar
      - n minutes bar?
  - [ ] category list
  - [ ] category daily bar
  - [ ] index list
  - [ ] index daily bar
  - [ ] stock dividend info
  - [ ] stock structure
  - [ ] stock fin reports
      - 资产负债表hist
      - 利润表hist
      - 现金流量表hist
      - 资产负债表now
      - 利润表now
      - 现金流量表now      
  - [ ] stock shareholder info
  - [ ] 港股通,沪股通资金流向及持仓 
  - [ ] category 资金流 *
  - [ ] index 资金流 *
  - [ ] stock news & Announcement *
  - [ ] stock 资金流 **  ->数据源已找到,参见postman-collection-richmind-key apis-☆ 东方财富_资金流向API
        ![](https://i.imgur.com/zJHzcce.png)
  - [ ] stock 转股(如股票改号码,股票A转成股票B) **
  - [ ] 基金数据 **
  
        * 有时间就完成
        ** stage1 不需完成,留待stage2 
+ stock overview web page 
+ buy and sell evaluator
+ all kinds of event evaluator


# 3.Stage1 tasks
## 3.1 Sensors
+ stock K line data
+ stock volume data
+ stock holder data
+ category/index data
+ stock basic info
+ stock dividend info
+ stock fin reports
+ stock structure hist

## 3.2 Evaluators
+ stock overview web page
+ single stock buy and sell evaluator
+ K line analysis
+ common figure analysis
+ common event analysis