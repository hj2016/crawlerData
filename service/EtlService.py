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
            EtlDao.EtlDao().saveMktEqud(sql)

        #query all stock
        count,result=self.etlDao.findAllSecIDs()
        sizenum=50
        num=count//sizenum
        for i in range(num+1):
            if num+1==i:
                tmp=result[i*sizenum:count]
            else:
                tmp=result[i*sizenum:(i+1)*sizenum]

            tickers=reduce(lambda x,y:x+","+y,map(lambda x:x["ticker"],tmp))
            saveData(tickers,startDate,endDate)

            #t=threading.Thread(target=saveData, args=(tickers,startDate,endDate,))
            #t.start()

        #t.join()
        return ""




if __name__ == '__main__':
    Logger.Logger.initLogger()
    e=EtlService()
    result=e.mktEqudDataSave("20060101","20091231")
    #result=e.mktEqudDataSave("20160401","20160618")
    print result