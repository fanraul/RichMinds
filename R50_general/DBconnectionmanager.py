from sqlalchemy import create_engine
from R50_general.general_constants import ls_hostname_DEV,ls_hostname_PRD,ls_hostname_OLAP
import socket
# remote database
# DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind_Remote'
# local database

RICHMINDS_DEV_conn_str = 'mssql+pyodbc://Richmind:121357468@Richmind'
RICHMINDS_OLAP_conn_str = 'mssql+pyodbc://Richmind:121357468@richmind@richmind_OLAP'
RICHMINDS_PRD_conn_str = 'mssql+pyodbc://Richmind:121357468@richmind@Richmind_PRD'
RICHMINDS_HF_PRD_conn_str = 'mssql+pyodbc://Richmind:121357468@richmind@RICHMIND_HF_PRD'

hostname = socket.gethostname()
if hostname in ls_hostname_DEV:
    DB_connection_string = RICHMINDS_DEV_conn_str
    HF_DB_connection_string = DB_connection_string
elif hostname in ls_hostname_PRD:
    DB_connection_string = RICHMINDS_PRD_conn_str
    # HF_DB_connection_string = 'mssql+pyodbc://Richmind:121357468@HF_Richmind_PRD'
    # TODO: 目前HF数据库是内网,prd无法连接,同时prd也无需连接HF,故用他自己的链接作为HF链接,保证不报错进行.
    HF_DB_connection_string = RICHMINDS_PRD_conn_str
# elif hostname in ls_hostname_HF_PRD:
#     DB_connection_string = RICHMINDS_PRD_conn_str
#     HF_DB_connection_string = RICHMINDS_HF_PRD_conn_str
elif hostname in ls_hostname_OLAP:
    DB_connection_string = RICHMINDS_OLAP_conn_str
    HF_DB_connection_string = RICHMINDS_HF_PRD_conn_str
else:
    assert 0==1,'please update the hostname to decide which SQL server to link!'


class Dbconnectionmanager:
    def __init__(self, db_conn_str = None,echo=False):
        if db_conn_str:
            self.engine = create_engine(db_conn_str,echo = echo)
        else:
            self.engine = create_engine(DB_connection_string,echo = echo)
        self.conn = self.engine.connect()

    def getengine(self):
        return self.engine

    def getconn(self):
        return self.conn

    def closeconn(self):
        self.conn.close()

class HF_Dbconnectionmanager:
    def __init__(self, echo=False):
        self.engine = create_engine(HF_DB_connection_string,echo = echo)
        self.conn = self.engine.connect()

    def getengine(self):
        return self.engine

    def getconn(self):
        return self.conn

    def closeconn(self):
        self.conn.close()

def _get_prd_conn():
    dcm_sql = Dbconnectionmanager(db_conn_str =RICHMINDS_PRD_conn_str,echo=False)
    return dcm_sql.getconn()

if __name__ == '__main__':
    dbconmgr = Dbconnectionmanager(echo=True)
#get stock list
    import pandas as pd
    dfm_stockid = pd.read_sql_query('select * from stock_basic_info'
                             , dbconmgr.getengine())
    print(dfm_stockid)
    dbconmgr.closeconn()