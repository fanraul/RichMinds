TODO:
1. 修改gcf中的函数**get_last_trading_day**,CN市场改成从Tquant中获得.
2. Tushare的get_realtime_quotes:实时取得股票当前报价和成交信息函数与自由函数gcf.get_stock_current_trading_info_sina的比较与融合.
3. 如果将来日线表的数据量也越来越大,性能变慢,可以考虑用partition的功能, 用mod 10的方法分成10个分区,但是这个语句也要不同的partition在不同的物理硬盘上才有用处.
4. log文件进入数据库,方面分类查找
5. 对应高频数据,初始化需要花费大量时间,待初始化完成后,需确认后续增量数据的处理方式,目前想,主要还是手工每月或每周直接执行收集程序,无job? 还是要再写一个job?
6. windows平台有没有更好的计划执行平台,目前用的是windows默认的任务管理器,没有每次执行的日志之类的详细信息,不好用.


1. 日线数据检验与修复
2. futu 数据抽取job program
3. allotment cninfo加入main sensor
4. 更新数据库函数增加一个功能,考虑manual adjusted indicator,如果这个标识位X,则在更新数据库时,不会再更新有manual ajusted标识的数据行了,用于已经完成数据校验的table,后续手工修改的部分不会被数据抽取程序自动覆盖. 另一个办法是把校验好的数据放到一个新的表中,还有个办法是加一个时间范围,在某个时间范围之前的就不再自动被更新了.
5. 从同花顺读取分红数据
6. 比较cninfo和futuquant的配股信息是否一致
7. 从em choice中获得股本数据,并导入,并与现有的数据进行比较.现有数据是netease和sina,感觉都不是很靠谱
8. 后续对em choice的期初数据导入完成后,要新建一个sensor程序,专门针对em choice的数据导入,这个程序手工运行,可几个月运行一次,或每周运行一次.
9. 增加dailybar数据源cninfo,有股本信息和股票名称历史信息(这个很有用)

#2018-4-6
1. 振幅的计算逻辑:
    - emchoice, (high-low)/preclose
    - cninfo,(high-low)/low
   结论,明显emchoice的才是正确的
2. cninfo也存在丢数据的情况, 601607在2007的数据是缺失的,netease也有这个问题,难道他们是相同的数据源?
3. cninfo中的日线数据中的证券简称字段是保留股票当时的名称的,可以用于查看股票的历史名称.
4. dailybar比较总结
    - futuquant的数据质量最差,不再使用
    - netease的数据质量最好,其次是emchoice,最后是Tquant
    - cninfo的质量也还行
    - netease,cninfo和emchoice和tquant都存在缺数据的现象
    - 相比之下,有数据的情况下,netease的数据质量最高,错误率最小,如果netease和tquant是一致的,但是和cmchoice不一致,一般都是emchoice的错误
    - 当netease和emchoice一致,和tquant不一致时,一般是tquant的错误
    - cninfo有时是正确的,有时和emchoice一致,但是和netease/Tquant不一致,我在查看了choice金融终端的历史K线图后,感觉还是netease是正确的
5. dailybar检查的方法与遗留问题
    - 用程序比较后,错误一般分为三类,left only,right only,和both
    - left only和right only中药排除stockid doesn't exist的错误,由于有的数据源是不包含部分退市股票的历史数据的,所以这类错误没有处理的意义
    - 剩下的left only和right only一般就是缺数据,参照其他数据源补全即可
    - both的问题比较复杂,说明有具体有些字段有问题, 其中:
        - open,直接修改就行
        - low,high,还要记得修改amplitude字段
        - preclose,close,还要修改chg,pchg字段
        - amount,vol,直接修改,暂不修改turnover字段
        - turnover字段,本次检查不考虑这个字段
        - TRADESTATUS,注意复牌这个状态,最好再导入之前,检查现有这个状态,参照后在修改之
    - 最难处理的问题是preclose, 有些股改复牌日的preclose字段交易所设置的并不合理(没有考虑股改复权),但是目前也暂不考虑复权之类的,直接按照交易所的公开数据维护preclose,
6. 具体处理log参见dailybar_compare_log all 2005-1-1~20180331.xlsx
7. 


#2018-4-3

1. dailybar数据清理,不同数据源处理数据的特殊逻辑.
    - 360改代码事件: 601313改代码成601360, 
        - netease的dailybar还是保留在原来601313上, 同时dialybar也放到601360下面,有一段时间的overlap;
        - tquant的dailybar还是保留原来601313的dailybars, 知道在2-28日改名发生后,dialybar转到了601360下面,无overlap;
        - 而choice则把601313的历史记录全部转到了601360上面了,而601313就没有dailybar记录了!,
      (感觉还是Tquant的做法最正确.但是choice目前的处理方式对后续处理的影响最小.)
    - netease的dailybar中前收盘数据会考虑复权(用的是复权后金额),change rate也是考虑复权后的,而futuquant的dailybar的PCHG就是简单的数学运算,不考虑复权
    - em choice的dailybar中前收盘数据会考虑复权(用的是复权后金额),change rate也是考虑复权后的
    - netease的复权数据似乎完全是按照交易所的值的,不考虑股权分置改革导致的分红.
2. emchoice/netease数据逻辑:
    - CHG = close -preclose (preclose是复权后的前交易日close)
    - PCHG = CHG/preclose


#2018-4-2
1. 从eastmoney的choice金融终端获得日线数据,并导入数据库. 
2. 经测试,无法使用sql server的bulk insert方法, 似乎和python生成的数据文件有格式上的不兼容(python write 生成的文件),还是使用df2db中的现有函数导入数据库
3. dailybar 比较,经过实际比较分析后,确认只比较,OCHL,vol,amount. turnover由于netease和em choice的差异过大,暂时不进行检查.
4. 现阶段完善数据的范围是从2005-1-1 到2018-3-31, 数据对象是:
    1. 日线数据,
    2. 分红配送数据
    3. 股本数据.

5. 初步比较的结果:
    - tquant vs emchoice: tquant要补2018-3-12,2018-3-13,2018-3-14三天的数据
    - netease vs emchoice: netease 要补2017-11-20,2018-1-2两天的数据
    - futuquant vs emchoice:


#2018-3-18
1.关于硬件架构的思考:
    - 阿里云的服务器作为OLTP数据抓取程序的运行端与数据库(2核4G内存),基本已经达到极限了.其作为futu的服务器,只复制日线部分的数据
    - 本地建立服务器作为OLAP,数据使用sql compare和sql data compare保持与服务器同步,其中:
        - 测试要用到的数据,才进行同步,每周一次或ad hoc,目前就是股票基本信息,和除权信息
        - 日线数据可以作为job,每周由本地服务器自行抓取
        - 没用到的数据不进行同步,如财务数据,板块数据
        - 高频数据也转移到该服务器,每两周或每月执行一次数据抓取
        - futuquant,分钟线级别数据在该服务器保存.

#2018-3-10 ~ 2018-3-17
1. 安装新的本地服务器.
2. 安装父母的电脑

#2018-3-9
1. 复权数据初步研究: 
    1. cninfo和同花顺的复权数据是一致的,futuquant和eastmoney的复权数据是一致,
    2. cninfo和同花顺复权是考虑股权分置改革的
    3. futuquant和eastmoney分红复权数据是不考虑股权分置改革的
    4. 交易所的涨跌幅也是不考虑股权分置改革的,虽然这个感觉很不合理


#2018-3-8
1. futuquant adjust info数据抓取上线
2. futu的数据(日线,分钟线,复权信息)抓取由于必须要等futu客户端更新完毕才能执行,故建议每周日再确认数据更新已完成后手工执行.
3. cninfo allotment和dividend上线和job部署.
4. cninfo dividend program use 股权登记日 as key due to some stocks fh doesn't have 除权基准日 info,maybe wrong record need to fix.to simplily scrap the web and loaded into DB, use 股权登记日 as key can ealisy avoid this problem,before use, user need to double check the 除权基准日 and fix it if required.
5. cninfo allotment使用除权基准日为key, 
6. 对于复权,除权基准日才是最重要的信息.


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