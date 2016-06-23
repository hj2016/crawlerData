# -*- coding: UTF-8 -*-
import ConfigParser
import sys
import logging
import MySQLdb
import time
from DBUtils.PooledDB import PooledDB
from MySQLdb.cursors import DictCursor
from util.DateUtil import DateUtil


class Mysql():
    """
        MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现
        获取连接对象：conn = Mysql.getConn()
        释放连接对象;conn.close()或del conn
    """
    # 连接池对象
    __pool = None

    def __init__(self):
        """
        数据库构造函数，从连接池中取出连接，并生成操作游标
        """
        self._conn = Mysql.__getConn()
        self._cursor = self._conn.cursor()

    @staticmethod
    def __getConf():
        thePath = sys.path[0]
        thePath = thePath[:thePath.find("crawlerData") + 11]
        CONF_MYSQL = thePath + "/resources/conf/Mysql.conf"
        cf = ConfigParser.ConfigParser()
        cf.read(CONF_MYSQL)
        return cf

    @staticmethod
    def __getConn():
        """
        @summary: 静态方法，从连接池中取出连接
        @return MySQLdb.connection
        """
        if Mysql.__pool is None:
            cf = Mysql.__getConf()
            __pool = PooledDB(creator=MySQLdb, mincached=1, maxcached=20,
                              host=cf.get("mysqldb", "host"), port=int(cf.get("mysqldb", "port")),
                              user=cf.get("mysqldb", "user"), passwd=cf.get("mysqldb", "passwd"),
                              db=cf.get("mysqldb", "db"), use_unicode=False, charset=cf.get("mysqldb", "charset"),
                              cursorclass=DictCursor)
        return __pool.connection()

    @DateUtil.time_me()
    def getAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchall()
        else:
            result = False
        return count, result

    @DateUtil.time_me()
    def getOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchone()
        else:
            result = False
        return result

    @DateUtil.time_me()
    def getMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        if count > 0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    @DateUtil.time_me()
    def insertOne(self, sql, value=None):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        logging.info("sql execute:" + sql)
        if value is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, value)
        return self.__getInsertId()

    @DateUtil.time_me()
    def insertMany(self, sql, values=None):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        # logging.info("sql execute:"+sql)
        logging.info("sql execute:...............")
        if values is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.executemany(sql, values)
        return count

    @DateUtil.time_me()
    def __getInsertId(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

    @DateUtil.time_me()
    def __query(self, sql, param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql, param)
        return count

    @DateUtil.time_me()
    def update(self, sql):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self._cursor.execute(sql)

    @DateUtil.time_me()
    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    @DateUtil.time_me()
    def dorp(self, table):
        return self.__query("drop table if exists " + table)

    @DateUtil.time_me()
    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    @DateUtil.time_me()
    def end(self, option='commit'):
        """
        @summary: 结束事务
        """
        if option == 'commit':
            self._conn.commit()
        else:
            self._conn.rollback()

    @DateUtil.time_me()
    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.end('commit')
        else:
            self.end('rollback');
        self._cursor.close()
        self._conn.close()

if __name__ == '__main__':
    mysql = Mysql()
    count,result = mysql.getAll("select * from stock_etl.stock_index_info;")
    mysql.dispose()
    print result
