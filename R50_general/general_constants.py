from datetime import datetime
import socket
# the earliset dailybar date time, in dev, it's currently it is 2014-1-1.
Global_dailybar_begin_date = datetime(2005,1,1).date()
Global_dailyticks_begin_datetime = datetime(2018,1,1)
Global_1minbar_begin_datetime = datetime(2017,2,1)

hostname = socket.gethostname()
ls_hostname_PRD = ['iZgaizjy01f4atZ',]
ls_hostname_DEV = ['BELLA-MACBUKAIR',]
ls_hostname_HF_PRD = ['TERRY-X200']


if hostname in ls_hostname_DEV:
    host_type = 'DEV'
elif hostname in ls_hostname_PRD:
    host_type = 'PRD'
elif hostname in ls_hostname_HF_PRD:
    host_type = 'HF_PRD'
else:
    assert 0==1,'please update the hostname into DEV list or PRD list'

if host_type == 'DEV':
    futu_api_ip = '101.132.98.4' # futu api global ip and port
    Global_path_news_details_jd = 'C:\\80_Business_docs\\news\\jd\\'
elif host_type == 'PRD':
    futu_api_ip = '127.0.0.1'
    Global_path_news_details_jd = 'D:\\80_Business_docs\\news\\jd\\'
elif host_type == 'HF_PRD':
    futu_api_ip = '101.132.98.4'
    Global_path_news_details_jd = 'C:\\80_Business_docs\\news\\jd\\'
else:
    assert 0==1,'unknown host type'


# futu_api_ip = '127.0.0.1'
futu_api_port = 11111

Global_Job_Log_Base_Direction = 'C:/00_RichMinds/log/'

Global_email_receiver = 'terry.fan@sparkleconsulting.com;fanraul@icloud.com'

tmp_output_path = 'C:\\00_RichMinds\\Github\\RichMinds\\ZZ_output\\'

weblinks = {
    'stock_list_easymoney': 'http://quote.eastmoney.com/stocklist.html',   # obselete
    'stock_change_record_qq': 'http://stock.finance.qq.com/corp1/profile.php?zqdm=%(stock_id)s',
    'stock_category_qq': 'http://stockapp.finance.qq.com/mstats/?mod=all', #obselete
    'stock_category_w_detail_qq': ["http://stock.gtimg.cn/data/view/bdrank.php?&t=%(catg_type)s/averatio&p=1&o=0&l=9999&v=list_data",
                                   "http://qt.gtimg.cn/q=%(catg_list)s",
                                   'http://stock.gtimg.cn/data/index.php?appn=rank&t=pt%(catg_code)s/chr&p=1&o=0&l=9999&v=list_data'],
    'stock_structure_sina':'http://vip.stock.finance.sina.com.cn/corp/go.php/vCI_StockStructure/stockid/%s.phtml',
    'stock_core_concept_eastmoney':"http://emweb.securities.eastmoney.com/PC_HSF10/CoreConception/CoreConceptionAjax?code=%s",
    'stock_shareholder_eastmoney':'http://emweb.securities.eastmoney.com/PC_HSF10/ShareholderResearch/ShareholderResearchAjax?code=%s',
    'stock_general_info_eastmoney':'http://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/CompanySurveyAjax?code=%s',
    'stock_dividend_cninfo':'http://www.cninfo.com.cn/information/dividend/%(market_id)s%(stock_id)s.html',
    'stock_dailybar_netease':'http://quotes.money.163.com/service/chddata.html?code=%s&start=%s&end=%s&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP',
    'stock_dailybar_sina':'http://hq.sinajs.cn/list=%s',
    'stock_newslist_jd':'http://gupiao.jd.com/index/newsList.html?pageSize=10&pageNum=%s', #沪深股票资讯
    'jd_stock_news_details_prefix':'http://gupiao.jd.com',
}


#table created by program dfm2table has prefix DD!
dbtables = {
    'finpreports_Tquant' :['DD_stock_fin_balance_tquant', 'DD_stock_fin_profit_tquant','DD_stock_fin_cashflow_tquant'],
    'name_hist_qq': 'DD_stock_name_change_hist_qq',
    'name_hist_Tquant' : 'DD_stock_name_change_hist_Tquant',
    'stock_category_relation_qq':'DD_stock_category_assignment_qq',
    'category_daily_trans_qq': 'DD_category_daily_noauth_qq',
    'stock_structure_sina':'DD_stock_structure_sina',
    'stock_core_concept_eastmoney':'DD_stock_core_concept_eastmoney',
    'stock_category_relation_eastmoney': 'DD_stock_category_assignment_eastmoney',  # TODO
    'stock_shareholder_number_eastmony': 'DD_stock_shareholder_number_eastmoney',
    'stock_top_ten_tradable_shareholder_eastmoney': 'DD_stock_shareholder_top_ten_tradable_eastmoney',
    'stock_top_ten_shareholder_eastmoney':'DD_stock_shareholder_top_ten_eastmoney',
    'stock_top_ten_shareholder_shares_changes_eastmoney':'DD_stock_shareholder_top_ten_shares_changes_eastmoney',
    'stock_fund_shareholder_eastmoney':'DD_stock_shareholder_fund_eastmoney',
    'stock_nontradable_shares_release_eastmoney':'DD_stock_shareholder_nontradable_shares_release_eastmoney',
    'stock_company_general_info_eastmoney':'DD_stock_company_general_info_eastmoney',
    'stock_company_issuance_info_eastmoney':'DD_stock_company_issuance_info_eastmoney',
    'stock_dividend_cninfo':'DD_stock_dividend_cninfo',
    'stock_dailybar_tquant': 'DD_stock_dailybar_Tquant',
    'stock_dailybar_futuquant':'DD_stock_dailybar_futuquant',
    'stock_fhsp_sina': 'DD_stock_fhsp_sina',  # TODO
    'stock_fhsp_eastmoney': 'DD_stock_fhsp_eastmoney',    # TODO
    'stock_dailybar_netease':'DD_stock_dailybar_netease',
    'stock_dailybar_sina': 'DD_stock_dailybar_sina',
    'newslist_jd':'DD_newslist_jd',
    'stocklist_hkus_futuquant':'DD_stocklist_hkus_futuquant',
    'stock_index_stocks_futuquant':'DD_stock_index_stocks_futuquant',
    'category':'ZCFG_category',
    'stock_category_stocks_futuquant':'DD_stock_category_stocks_futuquant',
    'stock_dailyticks_Tquant':'HF_%s_dailyticks_Tquant',
    'stock_1minbar_futuquant': 'HF_%s_1minbar_futuquant',
    'stock_1minbar_Tquant': 'HF_%s_1minbar_Tquant',
    'stock_adjinfo_futuquant':'DD_stock_adjinfo_futuquant',
    'stock_allotment_cninfo':'DD_stock_allotment_cninfo',
}
dbtemplate_stock_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC
))
"""
dbtemplate_stock_wo_date = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC
))
"""
dbtemplate_stock_date_multi_value = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Sqno] [int] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC,
	[Sqno] ASC
))
"""
dbtemplate_catg_date = """
CREATE TABLE [%(table)s](
	[Catg_Type] [nvarchar](50) NOT NULL,
	[Catg_Name] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Catg_Type] ASC,
	[Catg_Name] ASC,
	[Trans_Datetime] ASC
))
"""
dbtemplate_catg_date_multi_value_futuquant = """
CREATE TABLE [%(table)s](
	[Catg_Id] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Sqno] [int] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Catg_Id] ASC,
	[Trans_Datetime] ASC,
	[Sqno] ASC
))
"""
dbtemplate_jd_newslist = """
CREATE TABLE [%(table)s](
	[Region_ID] [nvarchar](50) NOT NULL,
	[News_datetime] [datetime] NOT NULL,
	[Title] [nvarchar] (200) NOT NULL,
	[News_FileID] [nvarchar] (30) NULL,
    [Weblink] [nvarchar] (100) NULL,
    [News_actual_datetime] [datetime] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
	[News_downloaded] [char](1) NULL,
	[Page_num_last_fetch] [int] NULL,
	[News_parsed] [char](1) NULL,
	[Ind_useless] [char](1) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Region_ID] ASC,
	[News_datetime] ASC,
	[Title] ASC
))
"""
dbtemplate_index_date_multi_value = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Index_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Sqno] [int] NOT NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY 
(
	[Market_ID] ASC,
	[Index_ID] ASC,
	[Trans_Datetime] ASC,
	[Sqno] ASC
))
"""

dbtemplate_HF_dailyticks = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[Sqno] [int] NOT NULL,
	[tick_Datetime] [datetime] NULL,
	[amount] [decimal](15, 2) NULL,
	[close] [decimal](8, 2) NULL,
	[opi] [decimal](8, 2) NULL,
	[vol] [decimal](15, 2) NULL,
	[买一价] [decimal](8, 2) NULL,
	[买一量] [decimal](15, 2) NULL,
	[卖一价] [decimal](8, 2) NULL,
	[卖一量] [decimal](15, 2) NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC,
	[Sqno] ASC
))
"""

dbtemplate_HF_1minbar_futu = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[open] [decimal](12, 4) NULL,
	[close] [decimal](12, 4) NULL,
	[high] [decimal](12, 4) NULL,
	[low] [decimal](12, 4) NULL,
	[vol] [decimal](15, 2) NULL,
	[amount] [decimal](15, 2) NULL,
	[PCHG] [decimal](12, 4) NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC
))
"""

dbtemplate_HF_1minbar_Tquant = """
CREATE TABLE [%(table)s](
	[Market_ID] [nvarchar](50) NOT NULL,
	[Stock_ID] [nvarchar](50) NOT NULL,
	[Trans_Datetime] [datetime] NOT NULL,
	[open] [decimal](12, 4) NULL,
	[close] [decimal](12, 4) NULL,
	[high] [decimal](12, 4) NULL,
	[low] [decimal](12, 4) NULL,
	[vol] [decimal](15, 2) NULL,
	[amount] [decimal](15, 2) NULL,
	[Created_datetime] [datetime] NULL,
	[Created_by] [nvarchar](50) NULL,
	[Last_modified_datetime] [datetime] NULL,
	[Last_modified_by] [nvarchar](50) NULL,
 CONSTRAINT [PK_%(table)s] PRIMARY KEY
(
	[Market_ID] ASC,
	[Stock_ID] ASC,
	[Trans_Datetime] ASC
))
"""

sqltemplate_set_compression = """
ALTER TABLE [%(table)s] REBUILD PARTITION = ALL
WITH 
(DATA_COMPRESSION = ROW
)
"""

sqltemplate_create_index_by_ticktime = """
CREATE NONCLUSTERED INDEX [IDX_%(mtkstk)s_Ticktime] ON [%(table)s]
(
	[tick_Datetime] ASC
)
"""


# the job sheduler for background programs
# key is the program name
# Notes: no more job set at Saturday, set them at Friday if required
#        merge Saturday into Friday from 2018/1/27
scheduleman = {
    'fetch_stock_fin_reports_from_tquant':{
        'rule':'W',
        'weekdays':[4]  # Friday
    } ,
    'fetch_stocklist_from_Tquant':{
        'rule': 'W',
        'weekdays': [0, 1, 2, 3, 4]  # monday to Friday
    },
    'fetch_stock_core_concept_from_eastmoney':{
        'rule': 'W',
        'weekdays': [1, 3]  # Tuesday, Thursday
    },
    'fetch_stock_structure_hist_from_sina':{
        'rule': 'W',
        'weekdays': [4]  # Friday
    },
    'fetch_stock_shareholder_from_eastmoney':{
        'rule': 'W',
        'weekdays': [0, 4]   # monday, Friday
    },
    'fetch_stock_company_general_info_from_eastmoney':{
        'rule': 'W',
        # due to program fetch_stock_change_record_from_qq doesn't work any more, this program can get the name changes as well
        # so make this program run every business day
        'weekdays': [0,1,2,3,4,6]  # monday to Friday, Sunday
    },
    'fetch_stock_dailybar_from_tquant':{
        'rule': 'W',
        'weekdays': [6, ]  # Sunday, run at 6:00PM friday can't get friday's dailybar, so change to sunday
    },
    'fetch_stock_dailybar_from_netease': {
        'rule': 'W',
        'weekdays': [0,1,2,3,4, ]  # monday to Friday
    },
    'fetch_stock_current_dailybar_from_sina': {
        'rule': 'W',
        'weekdays': [0,1,2,3,4, ]  # monday to Friday
    },
    'fetch_stock_dividend_from_cninfo':{
        'rule': 'W',
        'weekdays': [0, 3, ]  # Monday,Thursday
    },
    'fetch_stock_news_cn_from_jd':{
        'rule': 'W',
        'weekdays':[0,1,2,3,4,6]  #everyday except Saturday
    },
    'fetch_stocklist_hkus_from_futuquant':{
        'rule': 'W',
        'weekdays':[0,1,2,3,4,6]  # everyday except Saturday
    },
    'fetch_stock_category_from_futuquant': {
          'rule': 'W',
          'weekdays': [0, 1, 2, 3, 4, 6]  # everyday except Saturday
    },
    'fetch_stock_index_stocks_from_futuquant': {
          'rule': 'W',
          'weekdays': [0, 1, 2, 3, 4, 6]  # everyday except Saturday
    },
    'fetch_stock_change_record_from_qq': {
        'rule': 'W',
        'weekdays': []  # not scheduled any more due to web link doesn't work
    },
    'fetch_stock_category_and_daily_status_from_qq': {
        'rule': 'W',
        'weekdays': []
    # not scheduled any more due to poor performance(web block by qq) and poor data quality(don't use qq as data source)
    },
}

# the date which shouldn't run the job
excluded_dates =['2018-01-01',  # 格式必须是YYYY-MM-DD
                 '2019-01-01',
                 '2018-02-16',
                 '2018-02-17',
                 '2018-02-19',
                 '2018-02-20',
                 '2018-02-21',
                 ]


email_smtp_server = "smtp.163.com"
email_user_account = "fanraul@163.com"
email_user_pwd = "net121"