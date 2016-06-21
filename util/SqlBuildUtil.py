# -*- coding: UTF-8 -*-
__author__ = 'huang jing'
import Logger, Mysql
from dao import EtlDao


class SqlBuildUtil():
    INSERTSQLTMLP = "insert into ${table}(${column}) values "

    @staticmethod
    def insertBuild(table="", csvfile=""):
        if table is "" or csvfile is "":
            return None
        insertsql = SqlBuildUtil.INSERTSQLTMLP

        csvfiles = csvfile.split("\n")

        insertsql = insertsql.replace("${table}", table)
        insertsql = insertsql.replace("${column}", csvfiles[0])

        def mapfun(x):
            if (x is ""):
                return "null"
            else:
                return x

        if len(csvfiles[1:]) is 0:
            return None

        if csvfiles[1:][0] is not "":
            values = reduce(lambda x, y: x + "," + y,
                            map(lambda x: "(" + reduce(lambda x, y: x + "," + y, map(mapfun, x.split(","))) + ")",
                                filter(lambda x: x and x.strip(), csvfiles[1:])))

            insertsql = insertsql + values
            return insertsql

        return None


if __name__ == '__main__':
    Logger.Logger.initLogger()
    csvstr = 'id,age,name\n'
    result = SqlBuildUtil.insertBuild("user", csvstr)
    print result
