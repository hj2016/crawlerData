# -*- coding: UTF-8 -*-
import ConfigParser
import MySQLdb
import time
import os
import logging
from MySQLdb.cursors import DictCursor
from crawler.util.DateUtil import DateUtil
from crawler.util import Logger


class Mysql:
    u'''对MySQLdb常用函数进行封装的类'''

    error_code = ''  # MySQL错误号码

    _instance = None  # 本类的实例
    _conn = None  # 数据库conn
    _cursor = None  # 游标

    _TIMEOUT = 30  # 默认超时30秒
    _timecount = 0

    def __init__(self):

        u'构造器：根据数据库连接参数，创建MySQL连接'
        try:
            cf = Mysql.__getConf()
            self._conn = MySQLdb.connect(host=cf.get("mysqldb", "host"),
                                         port=int(cf.get("mysqldb", "port")),
                                         user=cf.get("mysqldb", "user"),
                                         passwd=cf.get("mysqldb", "passwd"),
                                         db=cf.get("mysqldb", "db"),
                                         charset=cf.get("mysqldb", "charset"),
                                         cursorclass=DictCursor)
        except MySQLdb.Error, e:
            self.error_code = e.args[0]
            error_msg = 'MySQL error! ', e.args[0], e.args[1]
            print error_msg

            # 如果没有超过预设超时时间，则再次尝试连接，
            if self._timecount < self._TIMEOUT:
                interval = 5
                self._timecount += interval
                time.sleep(interval)
                return self.__init__()
            else:
                raise Exception(error_msg)
        self._cursor = self._conn.cursor()
        self._instance = MySQLdb

    @staticmethod
    def __getConf():
        thePath = os.path.dirname(__file__)
        thePath = thePath[:thePath.find("crawler/") + 8]
        CONF_MYSQL = thePath + "resources/conf/Mysql.conf"
        cf = ConfigParser.ConfigParser()
        cf.read(CONF_MYSQL)
        return cf

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
    def fetchAllRows(self):
        u'返回结果列表'
        return self._cursor.fetchall()

    @DateUtil.time_me()
    def fetchOneRow(self):
        u'返回一行结果，然后游标指向下一行。到达最后一行以后，返回None'
        return self._cursor.fetchone()

    @DateUtil.time_me()
    def getRowCount(self):
        u'获取结果行数'
        return self._cursor.rowcount

    @DateUtil.time_me()
    def commit(self):
        u'数据库commit操作'
        self._conn.commit()

    @DateUtil.time_me()
    def rollback(self):
        u'数据库回滚操作'
        self._conn.rollback()

    @DateUtil.time_me()
    def __del__(self):
        u'释放资源（系统GC自动调用）'
        try:
            self._cursor.close()
            self._conn.close()
        except:
            pass

    @DateUtil.time_me()
    def close(self):
        u'关闭数据库连接'
        self.__del__()


if __name__ == '__main__':
    '''使用样例'''

    Logger.Logger.initLogger()
    # 连接数据库，创建这个类的实例
    db = Mysql()

    # 操作数据库
    sql = "SELECT * FROM tradeCal"
    count, result = db.getAll(sql)

    print result
    print count

    # 关闭数据库
    db.close()
