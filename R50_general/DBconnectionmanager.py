from sqlalchemy import create_engine
from R50_general.general_constants import ls_hostname_DEV,ls_hostname_PRD
import socket
# remote database
# DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind_Remote'
# local database

hostname = socket.gethostname()
if hostname in ls_hostname_DEV:
    DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind'
elif hostname in ls_hostname_PRD:
    DB_connection_string = 'mssql+pyodbc://Richmind:121357468@Richmind_PRD'
else:
    assert 0==1,'please update the hostname to decide which SQL server to link!'

class Dbconnectionmanager:
    def __init__(self, echo=False):
        self.engine = create_engine(DB_connection_string,echo = echo)
        self.conn = self.engine.connect()

    def getengine(self):
        return self.engine

    def getconn(self):
        return self.conn

    def closeconn(self):
        self.conn.close()

if __name__ == '__main__':
    dbconmgr = Dbconnectionmanager(echo=True)
#get stock list
    import pandas as pd
    dfm_stockid = pd.read_sql_query('select * from stock_basic_info'
                             , dbconmgr.getengine())
    print(dfm_stockid)
    dbconmgr.closeconn()