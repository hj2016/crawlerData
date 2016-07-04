# -*- coding: UTF-8 -*-
from crawler.util import Mysql

class BaseDao:
    def __init__(self):
        self.mysql = Mysql.Mysql()

    def delAllDate(self, table):
        self.mysql.delete(sql="truncate table " + table)
        self.mysql.dispose()

    def save(self, sql):
        reuslt = self.mysql.insertMany(sql)
        self.mysql.dispose()
        return reuslt

    def dropTable(self, table):
        reuslt = self.mysql.dorp(table)
        self.mysql.dispose()

    def query(self,sql):
        reuslt = self.mysql.getOne(sql)
        self.mysql.dispose()

