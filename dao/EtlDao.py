# -*- coding: UTF-8 -*-
from util import Mysql
class EtlDao:

    def findAllSecIDs(self):
        return self.mysql().getAll("select * from stock_etl.stockA_info")

    def saveMktEqud(self,sql):
        mysql=Mysql.Mysql()
        reuslt=mysql.insertMany(sql)
        self.mysql.dispose()
        return reuslt