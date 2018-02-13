import time
import urllib.error
import json.decoder
import http.client
from datetime import datetime
import os

import R50_general.dfm_to_table_common as df2db
from R50_general.general_helper_funcs import logprint
# from R50_general.general_helper_funcs import log_folder,log_job_name
import R50_general.general_helper_funcs as ghf
import R50_general.general_constants as gc
from http.client import BadStatusLine


def getweekday():
    weekday = datetime.utcnow().weekday()

    # overwrite weekday as certain date for re-processing only***********************
    # for example, reprocess at Saturday and assume it it Friday.
    # temp rule defined at 2018/1/27
    # rule: too much program dump at Friday, so treat Friday and Saturday as Friday,
    # so that reprocess can easily achieved at Saturday
    if weekday == 5:
        weekday = 4   # set Saturday as Friday
    #********************************************************************************
    return weekday

def auto_reprocess_dueto_ipblock(identifier:str,func_to_call,wait_seconds:int = 0):
    """
    sample usage:
     ahf.auto_reprocess_dueto_ipblock(identifier='fetch_stock_structure_hist_from_sina', func_to_call= fetch2DB, wait_seconds= 600)
    :param identifier:
    :param func_to_call:
    :param wait_seconds:
    :return:
    """
    # use a temp file to store the stockid processed
    # print(log_job_name)
    # print(ghf.log_job_name)
    if ghf.log_job_name == '':
        log_file_progressed_stockids = 'process_log_%s.txt' %identifier
    else:
        log_file_progressed_stockids = ghf.log_folder + ghf.log_job_name + '_' + 'process_log_%s.txt' %identifier
    append_log_file = open(log_file_progressed_stockids,'a')   #先用a模式生成文件,再用r模式读取文件,否则会报找不到文件的错误
    read_log_file = open(log_file_progressed_stockids,'r')
    ls_stockids_processed = [line.strip() for line in read_log_file.readlines()]
    # step2.1: get current stock list
    dfm_stocks_to_process = df2db.get_cn_stocklist('',ls_excluded_stockids=ls_stockids_processed)
    for index,row in dfm_stocks_to_process.iterrows():
        try:
            func_to_call(row['Stock_ID'])
            append_log_file.write(row['Stock_ID'] + '\n')
            append_log_file.flush()  #由于缓冲，字符串可能实际上没有出现在该文件中，直到调用flush()或close()方法被调用.
        except (urllib.error.HTTPError,urllib.error.URLError,http.client.RemoteDisconnected,ConnectionResetError,BadStatusLine):
            append_log_file.close()
            read_log_file.close()
            logprint('Web scrapping exception raised, auto reprocess after %s seconds. Current time is %s' % (wait_seconds,datetime.now()))
            time.sleep(wait_seconds)
            auto_reprocess_dueto_ipblock(identifier,func_to_call,wait_seconds)
            return
        except json.decoder.JSONDecodeError:
            append_log_file.close()
            read_log_file.close()
            logprint('JSONDecode exception raised, auto reprocess after %s seconds. Current time is %s' % (wait_seconds,datetime.now()))
            time.sleep(wait_seconds)
            auto_reprocess_dueto_ipblock(identifier,func_to_call,wait_seconds)
            return

    append_log_file.close()
    read_log_file.close()
    os.remove(log_file_progressed_stockids)

def isJobRun(program_name:str,processed_set:set)-> bool:
    # run or not based on the scheduler
    schedule = gc.scheduleman.get(program_name,{})
    if schedule:
        if schedule['rule'] == 'W':
            # 由于job每天晚上8点半才开始运行,有时12点后还在调试, 按UTC的时间,在第二天8点之前还算是昨天,可以仍然按照昨天的规则判断程序是否要执行.
            weekday = getweekday()
            if weekday in schedule['weekdays']:
                flg_jobrun = True
            else:
                flg_jobrun = False
        else:
            assert 0==1," Prod_id %s schedule rule unknown!" %program_name
    else:
        assert 0==1," Prod_id %s doesn't exist in general_constants.py as parameter: scheduleman " %program_name

    # run or not based on the processed list
    if flg_jobrun:
        if program_name in processed_set:
            flg_jobrun = False

    return flg_jobrun



def func_call_as_job_with_trace(func_name,*func_args,dt_args_w_name = {},program_name:str,processed_set=set()):
    if isJobRun(program_name,processed_set):
        func_call_with_trace(func_name, *func_args, dt_args_w_name =dt_args_w_name, program_name=program_name)

def func_call_with_trace(func_name,*func_args,dt_args_w_name = {},program_name:str = ''):
    """
    这个函数用于显示一个函数的开始执行和结束执行的日志并显示执行花费的时间(按秒显示)
    :param func_name:
    :param func_args:
    :param dt_args_w_name:
    :return:
    """
    start_time = datetime.now()
    logprint('*********         Start function call %s.%s     ***********' % (program_name,
                                                                              func_name.__name__))
    # print(*func_args)
    # print(**dt_args_w_name)
    func_name(*func_args,**dt_args_w_name)
    end_time = datetime.now()
    time_spent = end_time-start_time
    logprint('*********  End function call %s.%s, time spent: %d seconds  *************' % (program_name,
                                                                                            func_name.__name__,
                                                                                            time_spent.total_seconds()))