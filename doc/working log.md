TODO:
1. 修改gcf中的函数**get_last_trading_day**,CN市场改成从Tquant中获得.
2. Tushare的get_realtime_quotes:实时取得股票当前报价和成交信息函数与自由函数gcf.get_stock_current_trading_info_sina的比较与融合.
3. 如果将来日线表的数据量也越来越大,性能变慢,可以考虑用partition的功能, 用mod 10的方法分成10个分区,但是这个语句也要不同的partition在不同的物理硬盘上才有用处.
4. log文件进入数据库,方面分类查找
5. 对应高频数据,初始化需要花费大量时间,待初始化完成后,需确认后续增量数据的处理方式,目前想,主要还是手工每月或每周直接执行收集程序,无job? 还是要再写一个job?
6. windows平台有没有更好的计划执行平台,目前用的是windows默认的任务管理器,没有每次执行的日志之类的详细信息,不好用.

#2018-3-8
1. 日线数据检验与修复

#2018-3-7
1. cninfo 配股信息写入数据库
2. 复权因子数据写入数据库
3. cninfo 分红数据的Trans_datetie应该为除权基准日,不是股权登记日,要清空该表,重新执行数据抽取

#2018-3-6
1. dailybar check program 完成
2. 最近由于多个程序同时运行且都要访问数据库,101.132.98.4服务器经常宕机,看来还是要自己假设一个数据库服务器.
2. 3月5日邮件没有自动发送,虽然任务是成功完成了,今天要注意下邮件有没有收到.

#2018-3-5
1. Futuquant的1分钟线数据抽取程序完成,发现如下问题:
    1. futuquant的1分钟线返回值有时候会有重复记录出现,需要用drop_duplicate删除重复的trans_datetime记录.
    ![](https://i.imgur.com/ISJy75z.png)
    2. futuquant也有缺数据的现象,如下图2017/2/16日 9点51分的数据是缺失的.
    ![](https://i.imgur.com/Z1QHjua.png)
2. Futuquant的日线数据中的amount列数据是不全的,从2014年7月开始才有数据...

## 2018-3-3& 2018-3-4
1. Tquant的tick数据初始化中
2. Tquant的one-minutes数据程序编写完成,数据初始化中
3. db2df中对部分HF用到的函数增加了is_HF_conn的参数,用于确定这次db update是更新低频数据库还是高频数据库.
4. 1min的Tquant数据抽取发现有极少数数据是错误的比如下面同一时间有两条分钟线数据,同时少了一条分钟线数据,这时更新db会报错,目前的处理方式:
    ![](https://i.imgur.com/86FcD4v.png)
    1. db insert时检查is_HF_conn标识,如果是HF update,则只是记录错误,然后继续处理,不报异常
    2. 记录下来的错误要到tmp inconsistent log中去找,没有专门记录db错误的log文件

## 2018-3-2
1. 将所有的tick数据写入一张表被证实不可行,读取一个股票一个月的数据大约要花费5分钟才能完成, 800个股票一个月的ticks数据就已经达到了4千万的级别. 
2. 修改高频数据的存储逻辑,ticks和1minutes数据改为一个股票一张表的数据结构.

## 2018-3-1
1. 完成低频数据库的迁移,数据库和抽取程序服务器都使用101.132.98.4. done
2. 完成高频数据库的建立,数据库和抽取程序服务器都使用X200.
3. 进行daily ticks的数据抽取,数据起始日期为2018.1.1
4. 帮爸妈装机i3-8100
5. 买支持ddr3的h110板子
6. 

## 2018-2-28
1. 在阿里云上运行daily ticks tquant程序,感觉数据量极大且对服务器内存要求很高,故改变策略,决定将高频数据的服务器放置到本地,不再在阿里云上建立高频数据的服务器

## 2018-2-27
1. 完成Daily ticks Tquant程序

## 2018-2-21 - 2018-2-26
回湖南过年, do nothing

## 2018-2-19 & 2018-2-20
1. 测试Tushare提供的接口
    - 交易数据接口测试完毕
2. 完成每日sensor job 的excluded date逻辑,当当天日期再excluded date中维护过时,会自动终止job,不再执行.


## 2018-2-18
1. 在101.132.98.4上安装sql server
2. 101.132.98.4 完成所有历史数据的下载

## 2018-2-16 & 2018-2-17
1. 重新安装小机箱PC的win10操作系统
2. 整理3TB 硬盘的数据

## 2018-2-15
NA

## 2018-2-12 & 2018-2-13 & 2018-2-14
1. 完成测试Tquant自有行情接口数据并更新文档(onenote中).
2. 导入futuquant历史数据到futu客户端
3. 学习futuquant低频数据行情接口

## 2018-2-11 & 2018-2-10
1. 完成Tquant中从掘金量化myquant获得数据的行情数据接口的测试及文档工作.初步决定:
    - ticks info : need to use
    - dailybar: need to check futu to decide which resource to use
        - general bar: need to check futu to decide which resource to use
    - stock share info: need to use
    - stock market figure info: not use
    - 指数权重: need to use
    - financial figures: not use
    - stock_adj: not use
    - divident分红送配信息" need to use


## 2018-2-9
1. 修改**stock_basic_info**的列**Tquant_Market_ID**为**Tquant_symbol_ID**
2. 富途牛牛客户端在MAC电脑上的客户端无法使用问题已解决,解决人是_富途研发_Hugh(384862429)_. 方法是把ApiDiscla.dat放到 %appdata%\FTNN\1.0\Common里面再重启即可.
3. 重写部分Tquant的函数,研究Tquant的接口功能.

## 2018-2-8
1. 另外设置一个job **Richmind_index_with_no_stock_assigned_retry**,在每天的闲时(凌晨4点),对所有列**idx_exclusion_flg**标识为X的IDX进行查询,如果该IDX能找到所含的stocklist,则更新数据库并发邮件给我,然后我手工重置这个flg.
2. dialy job排除部分无stock的idx的查询操作上线
3. 新建一个view:**BD_L1_10_cn_stock_name_changes_hist**,用于查询股票的历史名称.
4. 今天开始处理stock K line数据
5. 学习myquant和tquant的文档

## 2018-2-7
1. 富途牛牛客户端在MAC电脑上的客户端无法使用,报错如下:
     ![](https://i.imgur.com/uNdTVPa.png)
2. 使用备选方案,链接阿里云上的富途牛牛客户端.已成功
3. 在**DD_stocklist_hkus_futuquant**中手工增加一列**manualflg_no_stocks_under_idx**,该列仅用于IDX类型, 为X时,不执行IDX所含stock list的查询(某些IDX类型,futu牛牛客户端提供不了其股票清单,会报错Failed	to get stocks	under idx	HK.100000. Err message: ERROR. timed out when	receiving	after sending 90 bytes. For req: {"Protocol":	1027,	Version:	1, ReqParam: {"Market": 1, StockCode: 100000}},同时浪费大量执行时间.
4. 手工更新列**manualflg_no_stocks_under_idx**,将无法取到数据的列进行排除.
5. build a general program **zz_update_db_from_excel** which load data from excel and convert them into an excel which contains a list of *update SQL statement* , this program can be used for manual db table update scenario


## 2018-2-6
1. program to get the cn market stock name changes is obsolete due to the web link doesn't work any more. so program **fetch_stock_change_record_from_qq** is obsolete as well
2. build another program **fetch_stock_name_changes_from_Tquant** to update the stock name change table inside program **fetch_stocklist_from_Tquant** after update stock list into db
3. build a general program **zz_insert_db_from_excel** which load data from excel and convert them into an excel which contains a list of *insert SQL statement* , this program can be used for manual db table insert scenario
4. deactive program **fetch_stock_category_and_daily_status_from_qq** in the *main sensor* daily job due to poor performance(web block by qq) and poor data quality
5. qq is bad data source for web scrap, the quality of stock infos in qq is poor, only scrap data from qq if the data is hist data can be scraped weekly eg. and there is no alternative data source.
6. manual prepare the gap data of stock name changes between 2018/1/13 and 2018/2/6 due to **fetch_stock_change_record_from_qq** is obsolete. The data may not be very accurate and I think it doesn't cause issue.
7. build a general function **exec_store_procedure** inside program **df2db** to call SQL server store procedure.
8. program **fetch_stock_name_changes_from_Tquant** go live in production.