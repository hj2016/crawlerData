# -*- coding: UTF-8 -*-
from util import Mysql
class EtlDao:
    def __init__(self):
        self.mysql=Mysql.Mysql()
    def findAllSecIDs(self):
        return self.mysql.findAll("stock_etl.stockA_info")

    def saveMktEqud(self,sql):
        return self.mysql.save(sql)