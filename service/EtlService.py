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
                tmp=result[i*50:count]
            else:
                tmp=result[i*50:(i+1)*50]

            tickers=reduce(lambda x,y:x+","+y,map(lambda x:x["ticker"],tmp))
            #result=GetDataUtil.GetDataUtil.getMktEqud(ticker=tickers,beginDate=startDate,endDate=endDate)
            #sql=SqlBuildUtil.SqlBuildUtil.insertBuild("mktEqud",result)
            #print sql
            #self.etlDao.saveMktEqud(sql)
            #start thread
            t=threading.Thread(target=saveData, args=(tickers,startDate,endDate,))
            t.start()

        return ""




if __name__ == '__main__':
    Logger.Logger.initLogger()
    e=EtlService()
    result=e.mktEqudDataSave("20160601","20160601")
    print result