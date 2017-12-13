# import sys
# sys.path.append(r"C:\00_RichMinds\Github\RichMinds")
# print(sys.path)

from datetime import datetime
import traceback
import os

from R10_sensor import (fetch_stock_fin_reports_from_tquant,
                        fetch_stocklist_from_Tquant ,
                        fetch_stock_category_and_daily_status_from_qq ,
                        fetch_stock_change_record_from_qq ,
                        fetch_stock_core_concept_from_eastmoney ,
                        fetch_stock_structure_hist_from_sina ,
                        fetch_stock_shareholder_from_eastmoney,
                        fetch_stock_company_general_info_from_eastmoney,
                        fetch_stock_dailybar_from_tquant,
                        fetch_stock_dividend_from_cninfo,
                        fetch_stock_dailybar_from_netease,
                        fetch_stock_current_dailybar_from_sina,
                        fetch_stock_news_list_from_jd,
                        )

import R50_general.general_helper_funcs as gcf
import R50_general.advanced_helper_funcs as ahf


def append_processed_prog_log(program_name:str):
    append_processed_prog_file.write(program_name + '\n')
    append_processed_prog_file.flush()  # 由于缓冲，字符串可能实际上没有出现在该文件中，直到调用flush()或close()方法被调用.


if __name__ == '__main__':
    gcf.setup_log_file('sensor_mainjob')
    # for job run, use utctime instead of local time so that before 8AM job rule applied as yesterday
    # 由于job每天晚上8点半才开始运行,有时12点后还在调试, 按UTC的时间,在第二天8点之前还算是昨天,可以仍然按照昨天的规则判断程序是否要执行.

    #如果要再周六重新运行程序,但是执行的是周五的job,则需要到getweekday函数中,overwrite weekday变量为4.
    weekday = ahf.getweekday()
    # in python, Monday is 0,Sunday is 6
    if weekday > 3:
        weekly_update = True
    else:
        weekly_update = False

    if weekly_update:
        # setup weekly_update specific parameters if required
        pass

    # a log file to record the program successfully run in one job run
    processed_prog_file = gcf.log_folder + 'sensor_mainjob_processed_program_log.txt'
    append_processed_prog_file = open(processed_prog_file, 'a')  # 先用a模式生成文件,再用r模式读取文件,否则会报找不到文件的错误
    read_processed_prog_file_file = open(processed_prog_file, 'r')
    processed_set = {line.strip() for line in read_processed_prog_file_file.readlines()}

    try:
        # step 10: update stock list
        ahf.func_call_as_job_with_trace(fetch_stocklist_from_Tquant.fetch2DB,
                                        program_name='fetch_stocklist_from_Tquant',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stocklist_from_Tquant')

        # step11: update stock daily bar current info from sina 股票每日交易数据
        ahf.func_call_as_job_with_trace(fetch_stock_current_dailybar_from_sina.auto_reprocess,
                                        program_name='fetch_stock_current_dailybar_from_sina',
                                        processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_current_dailybar_from_sina')

        # step 12: update stock company general info, 股本信息.
        ahf.func_call_as_job_with_trace(fetch_stock_company_general_info_from_eastmoney.auto_reprocess,
                                        program_name='fetch_stock_company_general_info_from_eastmoney',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_company_general_info_from_eastmoney')

        # step 20: get stock dailybar 股票每日蜡烛图数据
        ahf.func_call_as_job_with_trace(fetch_stock_dailybar_from_tquant.auto_reprocess,
                                 program_name='fetch_stock_dailybar_from_tquant',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_dailybar_from_tquant')

        # step 21: get stock dailybar 股票每日蜡烛图数据 网易
        ahf.func_call_as_job_with_trace(fetch_stock_dailybar_from_netease.auto_reprocess,
                                 program_name='fetch_stock_dailybar_from_netease',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_dailybar_from_netease')

        # step 25: get news list from jd
        # ahf.func_call_as_job_with_trace(fetch_stock_news_list_from_jd.fetch2DB,
        #                          program_name='fetch_stock_news_list_from_jd',processed_set=processed_set)
        # append_processed_prog_log(program_name='fetch_stock_news_list_from_jd')

        # step 30: update eastmoney core concept
        ahf.func_call_as_job_with_trace(fetch_stock_core_concept_from_eastmoney.auto_reprocess,
                                 program_name='fetch_stock_core_concept_from_eastmoney',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_core_concept_from_eastmoney')

        # step 40: update 3 fin reports
        ahf.func_call_as_job_with_trace(fetch_stock_fin_reports_from_tquant.auto_reprocess,
                                        program_name='fetch_stock_fin_reports_from_tquant',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_fin_reports_from_tquant')

        # step 50: update stock changes hist record, especially name
        ahf.func_call_as_job_with_trace(fetch_stock_change_record_from_qq.auto_reprocess,
                                 program_name='fetch_stock_change_record_from_qq',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_change_record_from_qq')

        # step 60: update stock shareholders info, 股东信息.
        ahf.func_call_as_job_with_trace(fetch_stock_shareholder_from_eastmoney.auto_reprocess,
                                 program_name='fetch_stock_shareholder_from_eastmoney',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_shareholder_from_eastmoney')

        # step 80: get stock divident 分红送转信息
        ahf.func_call_as_job_with_trace(fetch_stock_dividend_from_cninfo.auto_reprocess,
                                 program_name='fetch_stock_dividend_from_cninfo',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_dividend_from_cninfo')

        # step 90: update qq category info
        ahf.func_call_as_job_with_trace(fetch_stock_category_and_daily_status_from_qq.fetch2DB,
                                 program_name='fetch_stock_category_and_daily_status_from_qq',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_category_and_daily_status_from_qq')


        # step 900: update stock structure info, 股本信息.
        ahf.func_call_as_job_with_trace(fetch_stock_structure_hist_from_sina.auto_reprocess,
                                        program_name='fetch_stock_structure_hist_from_sina',processed_set=processed_set)
        append_processed_prog_log(program_name='fetch_stock_structure_hist_from_sina')



    except:
        append_processed_prog_file.close()
        read_processed_prog_file_file.close()
        gcf.send_daily_job_log('Job is failed, please check traceback log for job progress: %s' %traceback.print_exc(),flg_except=True)
        os._exit(1)

    append_processed_prog_file.close()
    read_processed_prog_file_file.close()
    os.remove(processed_prog_file)
    gcf.send_daily_job_log('Job is successfully finished, please check log file for details.')

#     dt_args_w_name={}
#     dt_args_w_name['sep'] = ','
#     func_call_with_trace('print','this is a test','hello',dt_args_w_name = dt_args_w_name)
#     func_call_with_trace('print_list_nice', [(1,2)]*1000000)