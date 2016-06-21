# -*- coding: UTF-8 -*-
from dao import BaseDao
from util import Mysql


class EtlDao(BaseDao.BaseDao):
    def __init__(self):
        self.mysql = Mysql.Mysql()

    def findAllSecIDs(self):
        count, result = self.mysql.getAll("select * from stock_etl.stockA_info")
        self.mysql.dispose()
        return count, result

    def updateStockInfoData(self):
        self.dropTable(table="stock_etl.stockA_info")
        count = self.mysql.update(
            sql="create table stock_etl.stockA_info as select * from tmp.secid where  substr(ticker,1,1) in('0','3','6')  and (exchangeCD='XSHE' or exchangeCD='XSHG') AND assetClass='E'")
        return count;
