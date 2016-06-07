# -*- coding: UTF-8 -*-
__author__ = 'huang jing'


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

        values=reduce(lambda x,y:x+","+y,map(lambda x: "("+x+")",csvfiles[1:]))

        insertsql=insertsql+values

        return insertsql

if __name__ == '__main__':
    csvstr = "id,age,name\n4,24,'hj'\n5,25,'xfw'"
    result=SqlBuildUtil.insertBuild("user",csvstr)
    print result
