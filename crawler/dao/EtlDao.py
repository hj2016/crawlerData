# -*- coding: UTF-8 -*-
from crawler.dao import BaseDao
from crawler.util import Mysql


class EtlDao(BaseDao.BaseDao):
    def __init__(self):
        self.mysql = Mysql.Mysql()

    def findAllSecIDs(self):
        count, result = self.mysql.getAll("select * from stock_etl.stock_a_info")
        self.mysql.close()
        return count, result

    def updateStockInfoData(self):
        count = self.mysql.update(
            sql="create table stock_etl.stock_a_info as select * from secid where  substr(ticker,1,1) in('0','3','6')  and (exchangeCD='XSHE' or exchangeCD='XSHG') and assetClass='E' and listStatusCD not in ('DE','UN')")
        self.mysql.close()
        return count;

    def findAllSecIDx(self):
        count, result = self.mysql.getAll("select * from stock_etl.stock_index_info")
        self.mysql.close()
        return count, result

    def findTradeCalBydate(self,tradeDate):
        sql = "select * from stock.tradeCal where calendarDate = '"+tradeDate+"'"
        result = self.mysql.getOne(sql)
        self.mysql.close()
        return result