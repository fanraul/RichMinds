import email
import email.mime.multipart as multipart  # import MIMEMultipart
import mimetypes
import os.path
import smtplib
import time
import urllib.error
import json.decoder
import urllib.request
import http.client
from datetime import datetime
from email.mime.base import MIMEBase  # import MIMEBase
from email.mime.text import MIMEText  # import MIMEText
import re
import sys

import numpy as np
import pandas as pd
import tushare as ts
from bs4 import BeautifulSoup
from pandas import DataFrame
from http.client import BadStatusLine

from R50_general import general_constants as gc

from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port


# global variable for log processing
log = True
log_folder = gc.Global_Job_Log_Base_Direction
log_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = ''
log_file_inconsistency = ''
log_job_name =''

def logprint(*args, sep=' ',  end='\n', file=None, add_log_files = '' ):
    """

    :param args:
    :param sep:
    :param end:
    :param file:
    :param add_log_files:'I': update in general log file and inconsistency log file
    :return:
    """
    global log_file,log_file_inconsistency
    # remove special unicode \u200b
    # args = [x.replace(u'\u200b','') for x in args]
    if log:
        print(*args,sep=' ', end='\n', file=None)
        if log_file == '':
            log_file = log_folder +'tmp/templog'+log_timestamp+'.txt'
            log_file_inconsistency = log_folder +'tmp/templog' + '_inconsistency_' +log_timestamp+'.txt'

        print(*args, sep=' ', end='\n', file= open(log_file,mode='a',encoding='UTF-8'))
        if 'I' in add_log_files:
            print(*args, sep=' ', end='\n', file=open(log_file_inconsistency,mode='a',encoding='UTF-8'))


def setup_log_file(jobname:str):
    global log_file,log_job_name,log_file_inconsistency
    log_job_name = jobname
    log_file = log_folder + jobname + '_' + log_timestamp + '.txt'
    log_file_inconsistency = log_folder + jobname + '_inconsistency' + '_' + log_timestamp + '.txt'

def get_webpage(weblink_str :str, time_wait = 0, flg_return_json= False,decode='',flg_return_rawhtml = False):
    """
    :param weblink_str: str of web link to web scrap
    :param time_wait: time to wait before open web link
    :param flg_return_json: True-> return str of repsonse,used in json senario
                            False-> return beautifulsoap object
    :return:
    """
    req = urllib.request.Request(weblink_str)
    user_agent = 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36'
    req.add_header('User-Agent', user_agent)
    req.add_header('Referer',weblink_str)
    try:
        time.sleep(time_wait)
        logprint('web page %s loading start..' %weblink_str)
        response = urllib.request.urlopen(req)
        html = response.read()
        logprint('web page loading end..')

        if flg_return_rawhtml:
            return html

        # leave it here for future if required.
        # sohu的网页编码有问题，直接解析会丢失html的body，要用errors等于ignore忽略解码错误后，再进行解析。
        # str_html = html.decode("gbk", errors='ignore')

        if flg_return_json:
            if decode == '':
                return html.decode(errors='ignore')
            else:
                return html.decode(decode, errors='ignore')
        soup = BeautifulSoup(html,"lxml")
    #        print (soup.prettify())
        return soup
    except urllib.error.HTTPError as e:
        logprint("The server couldn't fulfill the request.Page link is %s" % weblink_str)
        logprint('Error code: %s; Error Reason: %s' % (e.code, e.reason))
        raise e
        # if time_wait > 600:
        #     raise
        # logprint("Web page retry after %s..." %(time_wait+300))
        # get_webpage(weblink_str,time_wait + 300)
    except urllib.error.URLError as e:
        logprint('We failed to reach a server. Page link is %s' % weblink_str)
        logprint('Error Reason: %s' % (e.reason))
        raise e
        # if time_wait > 600:
        #     raise
        # logprint("Web page retry after %s..." %(time_wait+300))
        # get_webpage(weblink_str, time_wait + 300)
    except http.client.RemoteDisconnected as e:
        # TODO urgent
        logprint('Exception http.client.RemoteDisconnected raised, Remote end closed connection without response')
        raise e
    except ConnectionResetError as e:
        logprint('Exception ConnectionResetError raised, Remote end closed connection without response')
        raise e

    except BadStatusLine as e:
        logprint('Exception BadStatusLine raised, reason unknown?!')
        raise e

    except Exception as e:
        logprint("Weblink %s open failed error unkown:" % weblink_str, e,type(e))
        raise e

def get_webpage_with_retry(weblink_str :str, try_num = 5, ignore_error = True, time_wait = 0, flg_return_json= False,decode='',
                           flg_return_rawhtml = False,):
    for i in range(try_num):
        try:
            return get_webpage(weblink_str,time_wait,flg_return_json,decode,flg_return_rawhtml)
        except (urllib.error.HTTPError,urllib.error.URLError,http.client.RemoteDisconnected,ConnectionResetError):
            logprint('Web scrapping exception raised, auto reprocess after %s seconds. Current time is %s' % (time_wait,datetime.now()))
        except json.decoder.JSONDecodeError:
            logprint('JSONDecode exception raised, auto reprocess after %s seconds. Current time is %s' % (time_wait,datetime.now()))

    if ignore_error:
        logprint("%s can't be opened,please check!" %weblink_str,add_log_files='I')
        return None
    else:
        assert False, "%s can't be opened,please check!" %weblink_str

def dfm_col_type_conversion(dfm:DataFrame,index='',columns= {}, dateformat='%Y-%m-%d'):
    """
    this function is a general function to convert the columns in dataframe as the requested data type, it can also handle
    some general data conversion rule, such as '--' means None, but it shouldn't include some very specific rule(which should
    be handled by web scraper program itself.
    Caution: this function modify the dataframe directly, no return value.
    conversion rule includes:
    1) general rule for treat the data source as None.(eg. '--'), Note,put None into dataframe doesn't mean the data type will
       be None, there is no None in dataframe for number or datetime type, based on column data type, it will be converted to
        np.nan or pd.NaT
    2) for int type, convert float to int use round func. if u just use int(), then 1.99-> 1 or 2.
    3) for datetime type, convert string format to datetime format based on the format parameter.
    4) remove '万股' for float type,
    :param dfm:
    :param index: the data type of index column
    :param columns: a dict of the rquested data type of columns
    :param dateformat: for datetime type, what is the original date format for conversion
    :return:
    """
    # TODO: for 万股 processing, mulitple 10000 and use bigint type instead.
    def datetime_conversion(x):
        if isinstance(x,datetime):
            return x
        if x!= None and x != '--' and x != '' and x!='1900-1-1' and x!='1900-01-01':
            # print(x,type(x))
            return datetime.strptime(x, dateformat)
        else:
            return None

    def float_conversion(x):
        if not x:
            return x

        if type(x) == type('a'):
            if x == '--':
                return None
            elif len(x) > 2 and x[-2:] == '万股':
                return float(x[:-2])
        return float(x)
    #convert index
    if index == 'datetime':
        dfm['new_index_formated_999']= dfm.index.map(lambda x: datetime.strptime(x,dateformat))
        dfm.set_index('new_index_formated_999',inplace = True)

    def int_conversion(i):
        if pd.isnull(i) or i == '--' :
            return None
        if type(i) == str:
            return int(i)
        return(int(round(i,0)))

    def str_conversion(s):
        if s == 0:
            return '0'
        if pd.isnull(s) or s == '--':
            return None
        if s:
            return str(s)
        else:
            return None

    for col in columns:
        if col in dfm.columns:
            new_type = columns[col]
            if new_type == 'datetime':
                # ☆if no date or '1900-1-1' is specified, put None means no data specified!
                dfm[col] = dfm[col].map(datetime_conversion)
            elif 'varchar' in columns[col] or 'str' == columns[col] or 'char' in columns[col]:
                dfm[col] = dfm[col].map(str_conversion)
            elif 'int' in columns[col]:
                dfm[col] = dfm[col].map(int_conversion)
            elif 'float' in columns[col] or 'double' or 'decimal' in columns[col]:
                dfm[col] = dfm[col].map(float_conversion)

def print_list_nice(ls_tmp):
    print('List content is shown as below:')
    print("\n------------------------------\n".join(list(map(lambda x: str(x), ls_tmp))))
    print('List content Ends.')

def dfmprint(*args, sep=' ',  end='\n',  file=None):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth',255)
    pd.set_option('display.precision',8)
    print(*args, sep=' ',  end='\n',  file=None)

def determine_time_period_to_fetch_per_stock(mtkstk_id,last_fetch_datetime,first_issue_date,end_fetch_datetime,data_type):
    """
    return a tuple, (is_fetch_required,begin_time,end_time), is_fetch_required = Ture means data need to fetch.
                    false means no need to fetch.
    :param mtkstk_id:
    :param last_fetch_datetime:
    :param first_issue_date:
    :param end_fetch_datetime:
    :return:
    """
    if data_type == '1minbar':
        global_begin_datetime = gc.Global_1minbar_begin_datetime
    elif data_type == 'dailybar':
        global_begin_datetime = datetime.strptime(str(gc.Global_dailybar_begin_date) + ' 00:00', '%Y-%m-%d %H:%M')
    elif data_type == 'ticks':
        global_begin_datetime = gc.Global_dailyticks_begin_datetime
    else:
        assert 0==1, 'unknown data_type to fetch, no way to determine the begin and end datetime.'

    if last_fetch_datetime:
        begin_time = last_fetch_datetime
    elif not first_issue_date:
        logprint('No need to fetch stock transdata for stockid %s' % mtkstk_id, ' due to stock has no 上市日期')
        return False,None,None
    elif first_issue_date.date() > global_begin_datetime.date():
        begin_time = first_issue_date
    else:
        begin_time = global_begin_datetime

    end_time = end_fetch_datetime
    if begin_time > end_time:
        logprint('No need to fetch stock 1minbar for stockid %s' % mtkstk_id, ' due to stock 上市日期大于end_fetch_datetime')
        return False,None,None

    return True,begin_time,end_time

def isStrNumber(s:str):
    if s.isdigit():
        return True
    elif re.match('^[-+]?[0-9]+\.[0-9]+$',s):
        return True
    else:
        return False

def dfm_A_minus_B(A:DataFrame,B:DataFrame, key_cols:list)->DataFrame:
    """
    A - B return the entries which in A but not in B based on key cols, it is mainly used to remove duplicate entries and
    then insert to DB
    :param A:
    :param B:
    :return:
    """
    if len(B) == 0:
        return A
    # dfmprint(A[0:10])
    # dfmprint(B[0:10])
    B_tmp=B[key_cols].copy()
    B_tmp['tmp_col_duplicated'] = 'Y'
    dfm_merge_by_keycols = A.merge(B_tmp, how='left', on = key_cols)
    dfm_merge_by_keycols.fillna({'tmp_col_duplicated':'N'},inplace = True)
    dfm_dif_by_keycols = dfm_merge_by_keycols[dfm_merge_by_keycols.tmp_col_duplicated.isin(['N'])]
    del dfm_dif_by_keycols['tmp_col_duplicated']
    return dfm_dif_by_keycols
    # 不知道为什么,就是不能用A[A.col=='X']进行过滤,找到原因的,duplicated是个函数名,所以不能用其作为列名,有歧义!!
    # print(dfm_merge_by_keycols[dfm_merge_by_keycols.duplicated == 'X'])
    # del dfm_merge_by_keycols['duplicated']

def dfm_A_intersect_B(A:DataFrame,B:DataFrame, key_cols:list)->DataFrame:
    """
    A - B return the entries which in A and in B based on key cols, it is mainly used to identify duplicate entries and
    then update to DB
    :param A:
    :param B:
    :return:
    """
    if len(B) == 0:
        return DataFrame(columns=list(A.columns))
    B_tmp=B[key_cols].copy()
    B_tmp['tmp_col_duplicated'] = 'Y'
    dfm_merge_by_keycols = A.merge(B_tmp, how='left', on = key_cols)
    # dfm_merge_by_keycols.dropna() 这里不能使用dropna,dropna只要这行中有None值,就会删除.会导致误删除.
    dfm_merge_by_keycols = dfm_merge_by_keycols[dfm_merge_by_keycols.tmp_col_duplicated == 'Y']
    del dfm_merge_by_keycols['tmp_col_duplicated']
    return dfm_merge_by_keycols

def dfm_value_to_set(dfm_data:DataFrame,cols:list,float_fix_decimal) -> set:
    """
    convert dfm values of column cols into one set, {(value of col1,value of col2,...),(value of col1,...),...} each
    tuple represent for one line of dfm
    :param dfm_data:
    :param cols:
    :return:
    """
    def format_dfm_value_before_compare(x):
        # rule1: #将浮点数按固定进度(default位4位小数点转换成str类型.
        # rule2: #convert different types of None (pd.NaT, np.nan) to python None
        if isinstance(x, float) or isinstance(x, np.float32) or isinstance(x, np.float64) :
            return ('%.' + str(float_fix_decimal) + 'f') % x
        elif pd.isnull(x):
            return None
        else:
            return x

    ls_values =[]
    # check dfm_data is Series or Dataframe type
    # if Series:
    if isinstance(dfm_data,pd.Series):
        #将浮点数按固定进度(default位4位小数点转换成str类型.
        row_float_formated = [format_dfm_value_before_compare(x) for x in dfm_data[cols]]
        ls_values.append(tuple(row_float_formated))
    # if Dataframe
    elif isinstance(dfm_data,pd.DataFrame):
        for id in range(len(dfm_data)):
            # 将浮点数按固定进度(default位4位小数点转换成str类型.
            row_float_formated = [format_dfm_value_before_compare(x) for x in dfm_data.iloc[id][cols]]
            ls_values.append(tuple(row_float_formated))
    else:
        assert 0==1,'dfm_value_to_set parameter is not a type of Dataframe or Series!'
    # print(ls_values)
    return set(ls_values)

def setdif_dfm_A_to_B(dfm1,dfm2,cols:list,float_fix_decimal)->set:
    """
    return a set of difference for dfm1 to dfm2,meaning in dfm1 but not in dfm2 based on the column name list cols
    the set contains touples, each touple represent one line which in dfm1 but not in dfm2
    :param dfm1:
    :param dfm2:
    :return:
    """
    set_dfm1_values = dfm_value_to_set(dfm1, cols,float_fix_decimal)
    set_dfm2_values = dfm_value_to_set(dfm2, cols,float_fix_decimal)

    set_dif = set_dfm1_values - set_dfm2_values
    return set_dif

# def get_cn_market_calendar_tquant(startdate, enddate, market_id:str= '') ->list:
#     """
#
#     :param startdate:
#     :param enddate:
#     :param market_id:
#     :return: list of timestamp which is trading day
#     """
#     if market_id == '':
#         return (gt.get_calendar(startdate, enddate))


def get_last_trading_day(market_id:str='SH'):
    # TODO: currently only work for CN market, future support HK,US etc. market
    # gt.get_calendar result is incorrect, below 3 lines code is obsolete
    # last_trading_day = datetime.now()
    # while gt.get_calendar(last_trading_day,last_trading_day) :
    #     last_trading_day = last_trading_day - timedelta(days=1)

    # use index 000001 上证指数 to get the last tradin day
    if market_id in ('','SH','SZ'):
        dfm_000001 = ts.get_k_data('000001',index=True)
        if len(dfm_000001) == 0:
            assert False,'Can not get last trading day! '
        last_trading_day = datetime.strptime(dfm_000001.iloc[len(dfm_000001) - 1]['date'],'%Y-%m-%d')
    else:
        last_trading_day = datetime.strptime(get_trading_days_futuquant(market_id)[0],'%Y-%m-%d')

    return last_trading_day.date()

def get_last_trading_daytime(market_id:str='SH'):
    """
    return the end datetime for last trading day , if today, add actual datetime of today; else add '23:59:00'
    :param market_id:
    :return:
    """
    last_trading_day = get_last_trading_day(market_id)
    now = datetime.now().date()
    if last_trading_day == now:
        now_time = now.strftime('%H:%M')
        last_trading_daytime = datetime.strptime(str(last_trading_day) + ' ' + now_time, '%Y-%m-%d %H:%M')
    else:
        last_trading_daytime = datetime.strptime(str(last_trading_day) + ' 23:59', '%Y-%m-%d %H:%M')
    return last_trading_daytime

def get_trading_days_futuquant(market, start_date=None, end_date=None):
    '''

    :param market:市场标识
    :param start_date:起始日期;string类型，格式YYYY-MM-DD，仅指定到日级别即可，默认值None表示最近一年前的日期
    :param end_date:结束日期; string类型，格式YYYY-MM-DD，仅指定到日级别即可，取默认值None表示取当前日期
    :return: ret_code失败时，ret_data返回为错误描述字符串； 客户端无数据时，
             ret_code为成功，ret_data返回None,正常情况下，ret_data为日期列表（每个日期是string类型），如果指定时间段中无交易日，则ret_data为空列表。
    '''
    # 实例化行情上下文对象
    quote_ctx = OpenQuoteContext(api_ip, api_port)
    ret,ls_trading_days = quote_ctx.get_trading_days(market, start_date, end_date)
    if ret==RET_ERROR:
        assert 0==1,"Fatal error, Can't not get trading days for market %s, Err msg: %s" %(market,ls_trading_days)
    quote_ctx.close()
    return ls_trading_days

def send_email(receiver, title, content, attachments):
    From = "fanraul@163.com"
    To = receiver
    # server = smtplib.SMTP("smtp.163.com")
    server = smtplib.SMTP_SSL(gc.email_smtp_server)
    server.login(gc.email_user_account, gc.email_user_pwd)  # 仅smtp服务器需要验证时

    # 构造MIMEMultipart对象做为根容器
    main_msg = multipart.MIMEMultipart()

    # 构造MIMEText对象做为邮件显示内容并附加到根容器
    text_msg = MIMEText(content, _charset="utf-8")
    main_msg.attach(text_msg)

    # 构造MIMEBase对象做为文件附件内容并附加到根容器
    for attachment in attachments:
        file_name = attachment  # 附件名 :"c:/1.png"
        # 读入文件内容并格式化 [方式1]－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
        data = open(file_name, 'rb')
        ctype, encoding = mimetypes.guess_type(file_name)
        if ctype is None or encoding is not None:
            ctype = 'application/octet-stream'
        maintype, subtype = ctype.split('/', 1)
        file_msg = MIMEBase(maintype, subtype)
        file_msg.set_payload(data.read())
        data.close()
        email.encoders.encode_base64(file_msg)  # 把附件编码
        '''
         测试识别文件类型：mimetypes.guess_type(file_name) 
         rar 文件             ctype,encoding值：None None（ini文件、csv文件、apk文件） 
         txt text/plain None 
         py  text/x-python None 
         gif image/gif None 
         png image/x-png None 
         jpg image/pjpeg None 
         pdf application/pdf None 
         doc application/msword None 
         zip application/x-zip-compressed None 
        encoding值在什么情况下不是None呢？以后有结果补充。 
        '''
    # －－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－－
        ## 设置附件头
        basename = os.path.basename(file_name)
        file_msg.add_header('Content-Disposition', 'attachment', filename=basename)  # 修改邮件头
        main_msg.attach(file_msg)

    # 设置根容器属性
    main_msg['From'] = From
    main_msg['To'] = To
    main_msg['Subject'] = title
    main_msg['Date'] = email.utils.formatdate()

    # 得到格式化后的完整文本
    fullText = main_msg.as_string()

    # 用smtp发送邮件
    try:
        server.sendmail(From, To, fullText)
    except:
        raise
    finally:
        server.quit()

def send_daily_job_log(content:str,flg_except:bool = False):
    receiver = gc.Global_email_receiver
    if not flg_except:
        title = '%s job log %s' %(log_job_name,datetime.now().date())
    else:
        title = 'Daily job %s %s failed, please review the attachment and error text!' %(log_job_name,datetime.now().date())
        # TODO: error handling
    attachment = [log_file,log_file_inconsistency] if log_file_inconsistency and os.path.exists(log_file_inconsistency) else [log_file]
    content = content
    send_email(receiver, title, content, attachment)

def exception_handler():
    # TODO finish exception handler
    pass

def set_dict_misc_pars(char_origin,char_freq,allow_multiple,update_by,char_usage):
    dict_misc_pars = {}
    dict_misc_pars['char_origin'] = char_origin
    dict_misc_pars['char_freq'] = char_freq
    dict_misc_pars['allow_multiple'] = allow_multiple
    dict_misc_pars['created_by'] = dict_misc_pars['update_by'] = update_by
    dict_misc_pars['char_usage'] = char_usage
    return dict_misc_pars.copy()

def parse_chinese_uom(par:str):
    # print(par,len(par),type(par))
    if not par:
        return None
    if par == '--':
        return None
    if len(par) > 1:
        chinese_uom = par[-1:]
        if chinese_uom == '万':
            return floatN(par[:-1], '*', 10000)
        elif chinese_uom == '亿':
            return floatN(par[:-1], '*', 100000000)
        elif isStrNumber(par):
            return floatN(par)
    elif len(par) == 1:
        if isStrNumber(par):
            return floatN(par)
    assert 0==1, 'unknown format %s for chinese uom' %par


def floatN(x:str, oper:str ='*',y:float = 1):
    if not x:
        return None
    if type(x) == str:
        x = x.replace(',','')
        if x == '--':
            return None
    if oper == '*':
        return float(x) * y
    if oper == '/':
        return float(x) / y
    assert 0==1, 'unkown Operator!'

def market_id_conversion_for_cninfo(stockid:str):
    if stockid.startswith('6') :
        return 'shmb'
    if stockid.startswith('000') or stockid.startswith('001'):
        return 'szmb'
    if stockid.startswith('002'):
        return 'szsme'
    if stockid.startswith('3'):
        return 'szcn'

    assert False,'Can not determine market prefix of stockid %s for cninfo web linkage conversion!' %stockid

def intN(i:str):
    if not i:
        return None
    else:
        i = i.replace(',','')
        i = i.replace('.00','')
        return int(i)

def get_plate_stocks_futuquant(ip, port, strcode):
    quote_ctx = OpenQuoteContext(ip, port)
    ret, data_frame = quote_ctx.get_plate_stock(strcode)
    quote_ctx.close()
    return ret, data_frame

def get_cur_file_name_by_module_name(name):
    """
    use module's __name__ to get the file name without .py
    :param name:
    :return:
    """
    if name == '__main__':
        # it is main script, get file name from sys.argv[0]
        # samle:'C:\\Program Files\\JetBrains\\PyCharm Community Edition 2017.3.2\\helpers\\pydev\\pydevconsole.py'
        str_filename = sys.argv[0].split('/')[-1]
        return str_filename.split('.')[0]
    else:
        # it is run as a module, get file name from module2's varable __name__
        return name.split('.')[-1]

def get_tmp_file(filename):
    return gc.tmp_output_path + filename

def get_stock_current_trading_info_sina(mktstks,return_format,batch_size = 40):
    # TODO new functions need to be developed based on future usage, currently only work for one mktstk and return one dfm.
    """
    This function is a common interface to get stock current transaction info from SINA. Since it can be used in many ways,
    we use return_format to return the proper format to fit call program's request .
    :param mktstks: if string, it is one market_stockid, eg.'SZ300712',
                    if list, it is a list of mktstkids, eg. ['SZ300712','SZ300169']
    :return_format: a paramter to control the return parameter formating.
    :return: 'close_daily_fetch'-> return a dfm, index = col30(datetime format), value = col1 to col32,
    """

    if type(mktstks) == str:
        mktstks = [mktstks]

    mktskts = [x.lower() for x in mktstks]

    ls_col_name = ['股票名称',
                   'open',
                   '前收盘',
                   'close',
                   'high',
                   'low',
                   '竞买价',
                   '竞卖价',
                   'vol',
                   'amount',
                   '买一量',
                   '买一价',
                   '买二量',
                   '买二价',
                   '买三量',
                   '买三价',
                   '买四量',
                   '买四价',
                   '买五量',
                   '买五价',
                   '卖一量',
                   '卖一价',
                   '卖二量',
                   '卖二价',
                   '卖三量',
                   '卖三价',
                   '卖四量',
                   '卖四价',
                   '卖五量',
                   '卖五价',
                   '交易日期',
                   '交易时间',
                   'unknown',
                   ]

    ls_dfm_trans_info =[]
    ls_index_mktstk =[]
    ls_index_transdate = []
    ls_index_transdatetime =[]

    loopnum = len(mktskts) // batch_size
    for i in range(loopnum+1):
        mktstks_slice = mktskts[i*batch_size:(i+1)*batch_size]
        if mktstks_slice:
            str_mktstks = ','.join(mktstks_slice)
            url = gc.weblinks['stock_dailybar_sina'] %str_mktstks
            str_transinfos = get_webpage(url,flg_return_json=True,decode='gbk')
            ls_str_transinfo = str_transinfos.split(';\n')
            # print(ls_str_transinfo)
            for line_transinfo in ls_str_transinfo:
                dt_transinfo = {}
                line_transinfo = line_transinfo.replace('"','').replace('var hq_str_','')
                ls_tmp = line_transinfo.split('=')
                if len(ls_tmp) == 2:
                    ls_trans_dtl = ls_tmp[1].split(',')
                    str_mktstk = ls_tmp[0]
                    if ls_trans_dtl and len(ls_trans_dtl) == 33:
                        dt_transinfo ={x:y for x,y in map(lambda x,y: (x,y),ls_col_name,ls_trans_dtl)}
                        dt_transinfo['数据刷新时间'] = ls_trans_dtl[30]+' '+ls_trans_dtl[31]
                        ls_dfm_trans_info.append(dt_transinfo)
                        ls_index_mktstk.append(str_mktstks.upper())
                        ls_index_transdate.append(datetime.strptime(ls_trans_dtl[30],'%Y-%m-%d'))
                        ls_index_transdatetime.append(datetime.strptime(ls_trans_dtl[30]+' '+ls_trans_dtl[31],'%Y-%m-%d %H:%M:%S'))


    if len(ls_dfm_trans_info) > 0:
        if return_format == 'close_daily_fetch':
            return DataFrame(ls_dfm_trans_info,index=ls_index_transdate)
        else:
            assert False, 'return_format %s not yet developed!' %return_format
    else:
        return DataFrame()

'''
    var hq_str_sh600461="洪城水业,6.380,6.380,6.210,6.390,6.200,6.200,6.210,4541360,28556823.000,44200,6.200,8700,6.190,13600,6.180,2000,6.170,2000,6.160,2200,6.210,8300,6.220,14900,6.230,19472,6.240,19700,6.250,2017-11-17,15:00:00,00";
    var hq_str_sz300712="永福股份,33.090,33.020,30.150,33.580,29.900,30.140,30.150,7158146,224001829.750,10900,30.140,4800,30.130,2400,30.120,11200,30.100,2400,30.080,16467,30.150,1200,30.160,3600,30.180,5100,30.190,2700,30.200,2017-11-17,16:29:03,00";
    
    大秦铁路,8.800,8.790,9.030,9.060,8.700,9.030,9.040,109367311,971205411.000,73530,9.030,895919,9.020,555200,9.010,29700,9.000,137700,8.990,256000,9.040,772531,9.050,1108457,9.060,269100,9.070,248000,9.080,2017-11-17,15:00:00,00
    0：”大秦铁路”，股票名称；
    1：”27.55″，open；
    2：”27.25″，前收盘；
    3：”26.91″，close；
    4：”27.55″，high；
    5：”26.20″，low；
    6：”26.91″，竞买价；
    7：”26.92″，竞卖价；
    8：”22114263″，vol, 成交的股票数，由于股票交易以一百股为基本单位，所以在使用时，通常把该值除以一百；
    9：”589824680″，amount,成交金额，单位为“元”，为了一目了然，通常以“万元”为成交金额的单位，所以通常把该值除以一万；
    10：”4695″，“买一量”,申请4695股，即47手；
    11：”26.91″，“买一价”,报价；
    12：”57590″，“买二量”
    13：”26.90″，“买二价”
    14：”14700″，“买三量”
    15：”26.89″，“买三价”
    16：”14300″，“买四量”
    17：”26.88″，“买四价”
    18：”15100″，“买五量”
    19：”26.87″，“买五价”
    20：”3100″，卖一量,“卖一”申报3100股，即31手；
    21：”26.92″，“卖一价”
    (22, 23), 卖二量,卖二价 
    (24, 25), 卖三量,卖三价
    (26, 27), 卖四量,卖四价
    (28, 29), 卖五量,卖五价
    30：”2008-01-11″，交易日期；
    31：”15:05:32″，交易时间；
    32: "00",unkown col
'''

def get_mkt_stk_futuquant(code):
    # parse code of futuquant and return market_id and stock_id
    ls_code = code.split('.')
    if len(ls_code) == 2:
        return tuple(ls_code)
    elif len(ls_code) > 2:
        Market_ID = ls_code[0]
        Stock_ID = '.'.join(ls_code[1:])
        return (Market_ID,Stock_ID)
    else:
        assert 0 == 1, 'Wrong stock code: %s' % ls_code

if __name__ == "__main__":
    # print(get_cn_stocklist())
    # print(get_chars('Tquant',['FIN10','FIN20','FIN30']))
    # create_table_by_stock_date_template('hello123')
    # dict_misc_pars = {}
    # dict_misc_pars['char_origin'] = 'Tquant'
    # dict_misc_pars['char_freq'] = "D"
    # dict_misc_pars['allow_multiple'] ='N'
    # dict_misc_pars['created_by'] = 'fetch_stock_3fin_report_from_tquant'
    # dict_misc_pars['char_usage'] = 'FIN10'
    # add_new_chars_and_cols({'test1':'decimal(18,2)','test2':'decimal(18,2)'},[],'stock_fin_balance_1',dict_misc_pars)
    # dfm_temp = DataFrame([{'A':'1997-1-3','B':2,'C':'12.23'},{'A':'1997-1-4','B':3,'C':'12.35'}],index=['2017-1-1','2017-1-2'])
    # print(dfm_temp)
    # dfm_col_type_conversion(dfm_temp,index= 'datetime',columns={'A': 'datetime','B':'varchar(50)','C':'double(18,6)'})
    # print(dfm_temp)
    # print(list(dfm_temp.index))
    # print(type(dfm_temp.iloc[0][0]), type(dfm_temp.iloc[0][1]),type(dfm_temp.iloc[0][2]))
    # dfm1 = DataFrame([{'A':'A1','B':'B1','C':'C1'},{'A':'A2','B':'B2','C':'C2'},{'A':'A3','B':'B3','C':'C3'},])
    # dfm2 = DataFrame([{'A':'A1','B':'B1','C':'C1'},{'A':'A2','B':'B4','C':'C4'},{'A':'A3','B':'B3','C':'C5'},])
    # # dfm2 = DataFrame(columns = ['A', 'B', 'C', 'D'] )
    # print(dfm1,dfm2,sep='\n')
    # print(dfm_A_minus_B(dfm1,dfm2,key_cols = ['A','B']))
    # print(dfm_A_intersect_B(dfm1,dfm2,key_cols=['A','B']))

    # dfm4 = DataFrame(
    #     [{'A': 'A1', 'B': 'B1', 'C': 'C1'}, {'A': 'A2', 'B': 'B2', 'C': 'C2'}, {'A': 'A3', 'B': 'B3', 'C': 'C3'}, ])
    #
    # for index,row in dfm4.iterrows():
    #     print(index,type(index))
    #     print(row,type(row))

    # 7.test for get_last_trading_day
    # print(get_last_trading_day())
    # 8.test for func func_call_with_trace
    # dt_args_w_name={}
    # dt_args_w_name['sep'] = ','
    # func_call_with_trace(print,'this is a test','hello',dt_args_w_name = dt_args_w_name)
    # func_call_with_trace(print_list_nice, [(1,2)]*1000000)

    # 8.test for send_email
    send_email('terry.fan@sparkleconsulting.com;fanraul@icloud.com', 'email test',
               'this is a test for email attachement',
               []
               # ['C:/00_RichMinds/Github/RichMinds/sensor_mainjob.py','C:/00_RichMinds/Github/RichMinds/README.md']
               )

    # print(isStrNumber('123.3'))
    # print(isStrNumber('123.3.4'))
    # get_stock_current_trading_info_sina(['sz300712','sz300619'],return_format=1)

    # test module name get function
    # import R10_sensor.fetch_stock_category_from_futuquant
    # print(R10_sensor.fetch_stock_category_from_futuquant.global_module_name)


def pertentage_conversion(p:str):
    if not p:
        return None
    if p == '--':
        return None
    if p.endswith('%'):
        return float(p[:-1])/100
    return float(p)/100