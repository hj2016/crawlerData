# -*- coding: UTF-8 -*-
from dao import EtlDao
from util import Logger, GetDataUtil, SqlBuildUtil
import time, threading


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


if __name__ == '__main__':
    Logger.Logger.initLogger()
    e = EtlService()
    e.SecIDDataSave()
    # EtlDao.EtlDao().updateStockInfoData()
    # result = e.mktEqudDataSave("20100101", "20160621")
    # result=e.mktEqudDataSave("20160401","20160618")
