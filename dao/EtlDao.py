# -*- coding: UTF-8 -*-
from util import Mysql
class EtlDao:
    def __init__(self):
        self.mysql=Mysql.Mysql()

    def findAllSecIDs(self):
        count,result=self.mysql.getAll("select * from stock_etl.stockA_info")
        self.mysql.dispose()
        return count,result


    def saveMktEqud(self,sql):
        reuslt=self.mysql.insertMany(sql)
        self.mysql.dispose()
        return reuslt