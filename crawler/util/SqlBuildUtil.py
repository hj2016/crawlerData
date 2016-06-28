# -*- coding: UTF-8 -*-
__author__ = 'huang jing'
import Logger, Mysql
from crawler.dao import EtlDao


class SqlBuildUtil():
    INSERTSQLTMLP = "insert into ${table}(${column}) values "

    OHLCVCOLUMN = "secID,tickerName,tradeDate,open,high,low,close,volume,amount"

    @staticmethod
    def insertBuild(table, csvfile):
        if table is None or csvfile is None:
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

    @staticmethod
    def insertBuildxl(table, listfile):
        insertsql = SqlBuildUtil.INSERTSQLTMLP

        insertsql = insertsql.replace("${table}", table).replace("${column}", SqlBuildUtil.OHLCVCOLUMN)

        trdate = listfile[0]

        def mapfun(x):
            if (float(x[9]) == 0.0 and float(x[10]) == 0 and float(x[11]) == 0):
                return "('" + x[1] + "','" + x[2] + "','" + trdate + "'," + x[9] + "," + x[10] + "," + x[11] + "," + x[
                    8] + "," + \
                       x[12] + "," + x[13] + ")"
            else:
                return "('" + x[1] + "','" + x[2] + "','" + trdate + "'," + x[9] + "," + x[10] + "," + x[11] + "," + x[
                    3] + "," + \
                       x[12] + "," + x[13] + ")"

        map(mapfun, listfile[1])
        values = reduce(lambda x, y: x + "," + y, map(mapfun, listfile[1]))

        if (values is not ""):
            return insertsql + values
        else:
            return None

    @staticmethod
    def insertBuildts(table, column, tsvfile):
        if table is None or tsvfile is None:
            return None
        insertsql = SqlBuildUtil.INSERTSQLTMLP

        insertsql = insertsql.replace("${table}", table).replace("${column}", column)

        def mapfun(x):
            return "('" + reduce(lambda x, y: x + "','" + y, x) + "')"

        def reducefun(x, y):
            return x + "," + y

        values = reduce(reducefun, map(mapfun, tsvfile))
        insertsql = insertsql + values
        return insertsql


if __name__ == '__main__':
    Logger.Logger.initLogger()
    csvstr = '\tcode\tname\tc_name\n0\t600051\t宁波联合\t综合行业'
    result = SqlBuildUtil.insertBuildts("stock_etl.stock_industry", 'id,ticker,tickerName,tickerType', csvstr)
    print result
