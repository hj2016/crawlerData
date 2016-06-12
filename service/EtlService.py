# -*- coding: UTF-8 -*-
from dao import EtlDao
from util import Logger,GetDataUtil,SqlBuildUtil
import time, threading
class EtlService:
    def __init__(self):
        self.etlDao=EtlDao.EtlDao()

    def mktEqudDataSave(self,startDate,endDate):
        def saveData(tickers,startDate,endDate):
            result=GetDataUtil.GetDataUtil.getMktEqud(ticker=tickers,beginDate=startDate,endDate=endDate)
            sql=SqlBuildUtil.SqlBuildUtil.insertBuild("mktEqud",result)
            self.etlDao.saveMktEqud(sql)

        #query all stock
        count,result=self.etlDao.findAllSecIDs()

        num=count//50
        for i in range(num+1):
            if num+1==i:
                tmp=result[i*2:count]
            else:
                tmp=result[i*2:(i+1)*2]
            if i==1:
                tickers=reduce(lambda x,y:x+","+y,map(lambda x:x["ticker"],tmp))
                result=GetDataUtil.GetDataUtil.getMktEqud(ticker=tickers,beginDate=startDate,endDate=endDate)
                sql=SqlBuildUtil.SqlBuildUtil.insertBuild("mktEqud",result)
                print sql
                self.etlDao.saveMktEqud('insert into mktEqud(secID,ticker,secShortName,exchangeCD,tradeDate,preClosePrice,actPreClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,turnoverValue,dealAmount,turnoverRate,accumAdjFactor,negMarketValue,marketValue,PE,PE1,PB,isOpen) values ("000004.XSHE","000004","国农科技","XSHE","2016-06-02",36.83,36.83,0,0,0,36.83,0,0,0,0,1,3055486777,3092861861,4366.5477,-323.1848,39.0855,0) ')
                #start thread
                #t=threading.Thread(target=saveData, args=(tickers,startDate,endDate,))
                #t.start()

        return ""




if __name__ == '__main__':
    Logger.Logger.initLogger()
    e=EtlService()
    result=e.mktEqudDataSave("20160602","20160602")
    print result