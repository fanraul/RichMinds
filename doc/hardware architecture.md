# 2018-3-1 first design

## structure
1. one sql server to store low frequency data
2. one sql server to store high frequency data
3. one application server to fetch low frequency data
4. one application server to fetch high frequency data
5. one web server to support Wechat app

## computers
1. low frequency data fetched and stored in aliyun server 101.132.98.4 (2CPU 4GB)
2. high frequency data fetched and stored in local PC, temperary use X200(P8600 8GB)
3. need to buy a new server with high configuration (i7 + 16GB) to do the data analysis job
4. Wechat server (TBD)