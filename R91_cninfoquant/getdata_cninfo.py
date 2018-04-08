import time, json
from datetime import datetime
import urllib.request
import http.client
import pandas as pd
from pandas import DataFrame
from R50_general.general_helper_funcs import get_tmp_file,logprint,dfmprint

client_id, client_secret = "97760a8a9c914dacb8362298efad2b26", "53a44291c4934075ba1b49cd36d96cef"  # client_id,client_secret通过我的凭证获取

def gettoken(client_id, client_secret):
    url = 'http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token'
    post_data = "grant_type=client_credentials&client_id=%s&client_secret=%s" % (client_id, client_secret)
    post_data = post_data.encode()
    req = urllib.request.urlopen(url, post_data)
    responsecontent = req.read()
    responsedict = json.loads(responsecontent)
    token = responsedict["access_token"]
    # print(token)
    return token

def get_stock_dailybar(token,scode,s_datetime:datetime,e_datetime:datetime):   # 股票历史日行情
    token = gettoken(client_id, client_secret)

    sdate = s_datetime.strftime('%Y%m%d')
    edate = e_datetime.strftime('%Y%m%d')

    url = "http://webapi.cninfo.com.cn/api/stock/p_stock2402"  # 股票历史日行情 API
    post_data = "scode=%s&sdate=%s&edate=%s&access_token=%s" % (scode,sdate,edate,token)

    post_data =post_data.encode()
    req = urllib.request.urlopen(url, post_data)
    content = req.read()
    responsedict = json.loads(content)
    resultcode = responsedict["resultcode"]
    print(responsedict["resultmsg"], responsedict["resultcode"])
    if responsedict["resultmsg"] == "success":
        if len(responsedict["records"]) >= 1:
            # gcf.print_list_nice(responsedict["records"])          # 接收到的具体数据内容
            return True,DataFrame(responsedict["records"])
        else:
            return True,DataFrame()
    else:
        return False,responsedict["resultmsg"] + ',message code:' + responsedict["resultcode"]
