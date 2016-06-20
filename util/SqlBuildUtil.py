# -*- coding: UTF-8 -*-
__author__ = 'huang jing'
import Logger,Mysql
from dao import EtlDao
class SqlBuildUtil():
    INSERTSQLTMLP="insert into ${table}(${column}) values "

    @staticmethod
    def insertBuild(table="",csvfile=""):
        if table is "" or csvfile is "":
            return None
        insertsql=SqlBuildUtil.INSERTSQLTMLP

        csvfiles=csvfile.split("\n")

        insertsql=insertsql.replace("${table}",table)
        insertsql=insertsql.replace("${column}",csvfiles[0])

        def mapfun(x):
            if(x is ""):
                return "null"
            else:
                return x

        values=reduce(lambda x,y:x+","+y,map(lambda x: "("+reduce(lambda x,y:x+","+y,map(mapfun,x.split(",")))+")",filter(lambda x:x and x.strip(),csvfiles[1:])))

        insertsql=insertsql+values

        return insertsql

if __name__ == '__main__':
    Logger.Logger.initLogger()
    csvstr = 'id,age,name\n4,24,"简介"\n5,,"xfw"\n'
    result=SqlBuildUtil.insertBuild("user",csvstr)
    e=EtlDao.EtlDao()
    e.saveMktEqud('insert into mktEqud(secID,ticker,secShortName,exchangeCD,tradeDate,preClosePrice,actPreClosePrice,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,turnoverValue,dealAmount,turnoverRate,accumAdjFactor,negMarketValue,marketValue,PE,PE1,PB,isOpen) values ("000004.XSHE","000004","国农科技","XSHE","2016-06-02",36.83,36.83,0,0,0,36.83,0,0,0,0,1,3055486777,3092861861,4366.5477,-323.1848,39.0855,0) ')
    print result
