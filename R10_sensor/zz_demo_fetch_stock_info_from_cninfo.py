import R50_general.general_helper_funcs as gcf
import time, json
import urllib.request
import http.client
import pandas as pd
from pandas import DataFrame

zh_dailybar_cols = {'F001V':'交易所',
                    'F002N':'昨日收盘价',
                    'F003N':'今日开盘价',
                    'F004N':'成交数量',
                    'F005N':'最高成交价',
                    'F006N':'最低成交价',
                    'F007N':'最近成交价',
                    'F008N':'总笔数',
                    'F009N':'涨跌',
                    'F010N':'涨跌幅',
                    'F011N':'成交金额',
                    'F012N':'换手率',
                    'F013N':'振幅',
                    'F019N':'公司总股本',
                    'F020N':'发行总股本',
                    'F021N':'流通股本',
                    }

def gettoken(client_id, client_secret):
    url = 'http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token'  # api.before.com需要根据具体访问域名修改
    post_data = "grant_type=client_credentials&client_id=%s&client_secret=%s" % (client_id, client_secret)
    post_data = post_data.encode()
    req = urllib.request.urlopen(url, post_data)
    responsecontent = req.read()
    responsedict = json.loads(responsecontent)
    token = responsedict["access_token"]
    print(token)
    return token


def apiget(scode, tokent):
    url = "http://webapi.cninfo.com.cn/api/stock/p_stock2100?scode=%s&access_token=%s"  # apitest2.com需要根据具体访问域名修改
    conn = http.client.HTTPConnection("apitest2.com")
    conn.request(method="GET", url=url % (scode, tokent))
    response = conn.getresponse()
    rescontent = response.read()
    responsedict = json.loads(rescontent)
    resultcode = responsedict["resultcode"]
    print (responsedict["resultmsg"], responsedict["resultcode"])
    if (responsedict["resultmsg"] == "success" and len(responsedict["records"]) >= 1):
        print(responsedict["records"])  # 接收到的具体数据内容
    else:
        print( 'no data')

    return resultcode


def apipost(scode, tokent):
    url = "http://webapi.cninfo.com.cn/api/stock/p_stock2100"  # apitest2.com需要根据具体访问域名修改
    post_data = "scode=%s&access_token=%s" % (scode, tokent)
    post_data =post_data.encode()
    req = urllib.request.urlopen(url, post_data)
    content = req.read()
    responsedict = json.loads(content)
    resultcode = responsedict["resultcode"]
    print(responsedict["resultmsg"], responsedict["resultcode"])
    if (responsedict["resultmsg"] == "success" and len(responsedict["records"]) >= 1):
        print(responsedict["records"])          # 接收到的具体数据内容

    else:
        print('no data')
    return resultcode

def get_stock_dailybar(token,scode,sdate,edate):
    url = "http://webapi.cninfo.com.cn/api/stock/p_stock2402"  # apitest2.com需要根据具体访问域名修改
    post_data = "scode=%s&sdate=%s&edate=%s&access_token=%s" % (scode,sdate,edate,token)
    post_data =post_data.encode()
    req = urllib.request.urlopen(url, post_data)
    content = req.read()
    responsedict = json.loads(content)
    resultcode = responsedict["resultcode"]
    print(responsedict["resultmsg"], responsedict["resultcode"])
    if (responsedict["resultmsg"] == "success" and len(responsedict["records"]) >= 1):
        # gcf.print_list_nice(responsedict["records"])          # 接收到的具体数据内容
        return DataFrame(responsedict["records"])
    else:
        print('no data')
        return DataFrame()


if __name__ == "__main__":
    client_id, client_secret = "97760a8a9c914dacb8362298efad2b26", "53a44291c4934075ba1b49cd36d96cef"  # client_id,client_secret通过我的凭证获取
    token = gettoken(client_id, client_secret)
    # for i in range(0, 5):  # 注：3600为循环访问API的次数
    #     scode = str(300000 + i)  # 股票代码，根据自己需要传入
    #     print("processing %s" %scode)
    #     # resultcode=apiget(scode,token)   #以http get方法获取数据
    #     resultcode = apipost(scode, token)  # 以http post方法获取数据
    #     if resultcode == 405:  # token失效，重新获取
    #         token = gettoken(client_id, client_secret)
    #         apiget(scode, token)  # get请求
    #         # apipost(scode,token)#post请求
    #     time.sleep(1)
    scode = '600094,600313,600372,600401,600633,600681,600705,600727,600817,600898,601607'    # 股票代码，根据自己需要传入
    scode = '601607'
    scode = '600057,600094,600313,600372,600633,600681,600705,600727,600817,600898'
    scode = '600633,600681,600705,600727,600817,600898'
    scode = '600372,600633,600681,600727,600817,600898'
    scode = '600633,600681,600727,600817,600898'
    scode = '600313,600633,600727,600898'
    scode = '600885'
    scode = '600028,600241,600493,600577,600586,600802,603001'
    scode = '600822,603101,603117,603737,603799'
    scode = '600845'
    scode = '603123'
    scode = '600563,600828'
    scode = '000034,000036,000065,000505,000518,000576,000585,000586,000595,000607,000628,000702,000720,000727,000753,000819,000930,000955,000979,002002,002011,002012,002016,002036,002042,002046'
    scode = '600561'
    sdate = '20060426'
    edate = '20060426'
    dfm_records = get_stock_dailybar(token, scode, sdate, edate)
    dfm_records.rename(columns= zh_dailybar_cols,inplace=True )
    dfm_records = dfm_records[['SECCODE',
                               'TRADEDATE',
                               '昨日收盘价',
                               '今日开盘价',
                               '最近成交价',
                               '最低成交价',
                               '最高成交价',
                               '涨跌',
                               '涨跌幅',
                               '成交数量',
                               '成交金额',
                               '换手率',
                               '振幅',
                               '总笔数',
                               'SECNAME',
                               ]]
    gcf.dfmprint(dfm_records)
    dfm_records.to_clipboard()
    #     print("processing %s" %scode)
    #     # resultcode=apiget(scode,token)   #以http get方法获取数据
    #     resultcode = apipost(scode, token)  # 以http post方法获取数据



