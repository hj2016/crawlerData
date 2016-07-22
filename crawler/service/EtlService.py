# -*- coding: UTF-8 -*-
import datetime
from crawler.dao import EtlDao
from crawler.util import Logger, GetDataUtil, SqlBuildUtil
import time, threading
import tushare as ts


class EtlService:
    def __init__(self):
        self.etlDao = EtlDao.EtlDao()

    # get stock data
    def mktEqudDataSave(self, startDate, endDate):
        def saveData(tickers, startDate, endDate):
            result = GetDataUtil.GetDataUtil.getMktEqud(ticker=tickers, beginDate=startDate, endDate=endDate)
            sql = SqlBuildUtil.SqlBuildUtil.insertBuild("mktEqud", result)
            if sql is not None:
                EtlDao.EtlDao().save(sql)

        # query all stock
        count, result = self.etlDao.findAllSecIDs()
        sizenum = 50
        num = count // sizenum
        for i in range(num + 1):
            if num + 1 == i:
                tmp = result[i * sizenum:count]
            else:
                tmp = result[i * sizenum:(i + 1) * sizenum]

            tickers = reduce(lambda x, y: x + "," + y, map(lambda x: x["ticker"], tmp))
            saveData(tickers, startDate, endDate)

            # t=threading.Thread(target=saveData, args=(tickers,startDate,endDate,))
            # t.start()

        # t.join()
        return ""

    def SecIDDataSave(self):
        tablename = "tmp.secid"

        # 删除当前表数据
        EtlDao.EtlDao().delAllDate(tablename)

        secIDE = GetDataUtil.GetDataUtil.getSecID(assetClass="E")
        secIDB = GetDataUtil.GetDataUtil.getSecID(assetClass="B")
        secIDIDX = GetDataUtil.GetDataUtil.getSecID(assetClass="IDX")
        secIDFU = GetDataUtil.GetDataUtil.getSecID(assetClass="FU")
        secIDOP = GetDataUtil.GetDataUtil.getSecID(assetClass="OP")

        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, secIDE)
        EtlDao.EtlDao().save(sql)
        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, secIDB)
        EtlDao.EtlDao().save(sql)
        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, secIDIDX)
        EtlDao.EtlDao().save(sql)
        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, secIDFU)
        EtlDao.EtlDao().save(sql)
        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, secIDOP)
        EtlDao.EtlDao().save(sql)

        EtlDao.EtlDao().dropTable(table="stock_etl.stockA_info")
        EtlDao.EtlDao().updateStockInfoData()

    def idxDataSave(self):
        tablename = "idx"
        idx = GetDataUtil.GetDataUtil.getIdx()
        sql = SqlBuildUtil.SqlBuildUtil.insertBuild(tablename, idx)
        EtlDao.EtlDao().save(sql)

    def mktIdxdSave(self, startDate, endDate):
        count, result = EtlDao.EtlDao().findAllSecIDx()
        sizenum = 50
        num = count // sizenum
        for i in range(num + 1):
            if num + 1 == i:
                tmp = result[i * sizenum:count]
            else:
                tmp = result[i * sizenum:(i + 1) * sizenum]

            tickers = reduce(lambda x, y: x + "," + y, map(lambda x: x["ticker"], tmp))
            result = GetDataUtil.GetDataUtil.getMktIdxd(ticker=tickers, beginDate=startDate, endDate=endDate)
            sql = SqlBuildUtil.SqlBuildUtil.insertBuild("mktIdx", result)
            if sql is not None:
                EtlDao.EtlDao().save(sql)

    def dayStockData(self, dateFlag=True):
        result = GetDataUtil.GetDataUtil.getXlStockInfo()
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        dataResult = today, result[1]
        sql = SqlBuildUtil.SqlBuildUtil.insertBuildxl("stock_etl.stock_a_trans", dataResult)
        EtlDao.EtlDao().save(sql)

    def dayIndexData(self, dateFlag=True):
        result = GetDataUtil.GetDataUtil.getXlIndexInfo()
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        dataResult = today, result[1]
        sql = SqlBuildUtil.SqlBuildUtil.insertBuildxl("stock_etl.stock_index_trans", dataResult)
        EtlDao.EtlDao().save(sql)


    def industryData(self):
        table = 'stock_etl.stock_industry'
        column = 'ticker,tickerName,tickerType'
        result = ts.get_industry_classified()
        sql = SqlBuildUtil.SqlBuildUtil.insertBuildts(table, column, result.values)
        EtlDao.EtlDao().delAllDate(table)
        EtlDao.EtlDao().save(sql)

    def conceptData(self):
        table = 'stock_etl.stock_concept'
        column = 'ticker,tickerName,tickerType'
        result = ts.get_concept_classified()
        sql = SqlBuildUtil.SqlBuildUtil.insertBuildts(table, column, result.values)
        EtlDao.EtlDao().delAllDate(table)
        EtlDao.EtlDao().save(sql)

    def regionData(self):
        table = 'stock_etl.stock_region'
        column = 'ticker,tickerName,tickerType'
        result = ts.get_area_classified()
        sql = SqlBuildUtil.SqlBuildUtil.insertBuildts(table, column, result.values)
        EtlDao.EtlDao().delAllDate(table)
        EtlDao.EtlDao().save(sql)

    def isOpenTrade(self):
        today=time.strftime('%Y-%m-%d',time.localtime(time.time()))
        result = EtlDao.EtlDao().findTradeCalBydate(today)
        if result['isOpen']==1L:
            return True
        if result['isOpen']==0L:
            return False


if __name__ == '__main__':
    # Logger.Logger.initLogger()
    e = EtlService()
    # result = e.isOpenTrade()
    # print result

    e.regionData()

    # e.industryData()
    # e.conceptData()
    # e.regionData()
    # e.mktIdxdSave("20160629", "20160704")
    # e.mktEqudDataSave("20160629", "20160704")

    # e.dayIndexData()
    # e.mktIdxdSave("20000101", "20160621")
    # e.SecIDDataSave()
    # EtlDao.EtlDao().updateStockInfoData()
    # result = e.mktEqudDataSave("20100101", "20160621")
    # result=e.mktEqudDataSave("20160401","20160618")
