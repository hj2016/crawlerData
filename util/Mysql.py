# -*- coding: UTF-8 -*-
import MySQLdb
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import ConfigParser
import logging
import string, os, sys
from tornado import gen
from tornado_mysql import pools
from tornado_mysql.cursors import DictCursor


class DataSource(object):
    def __new__(cls, *args, **kwargs):
        return  super(DataSource, cls).__new__(cls, *args, **kwargs)
class Mysql(DataSource):

    __instance = None
    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super(Mysql, cls).__new__(cls, *args, **kwargs)
        return cls.__instance

    """
        MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现
        获取连接对象：conn = Mysql.getConn()
        释放连接对象;conn.close()或del conn
    """
    #连接池对象
    __pool = None

    #sql select all tample
    SELECTALL="select * from ${table}"

    def __init__(self):
        """
        数据库构造函数，从连接池中取出连接，并生成操作游标
        """
        self._conn = Mysql.__getConn()
        self._cursor = self._conn.cursor()
    @staticmethod
    def __getConf():
        thePath = sys.path[0]
        thePath = thePath[:thePath.find("crawlerData")+11]
        CONF_MYSQL = thePath+"/resources/conf/Mysql.conf"
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
            cf=Mysql.__getConf()
            __pool = PooledDB(creator=MySQLdb, mincached=1 , maxcached=20 ,
                              host=cf.get("mysqldb", "host") , port=int(cf.get("mysqldb", "port")) , user=cf.get("mysqldb", "user") , passwd=cf.get("mysqldb", "passwd") ,
                              db=cf.get("mysqldb", "db"),use_unicode=False,charset=cf.get("mysqldb", "charset"),cursorclass=DictCursor)
        return __pool.connection()

    def findAll(self,table=None):
        if table is None:
            logging.info("sql build table param is not None")
            raise AttributeError,("table param is not None","in Mysql findAll")

        findSql= Mysql.SELECTALL
        findSql=findSql.replace("${table}",table)
        logging.info("sql:"+findSql)

        result=""
        count=0
        try:
            count=self._cursor.execute(findSql)
            if count>0:
                result=self._cursor.fetchall()
        except Exception:
            logging.error("sql execute error")
        finally:
            logging.info("mysql close......")
            Mysql().dispose()

        return count,result

    def save(self,sql):
        count=0
        try:
            logging.info("sql:"+sql)
            count = self._cursor.execute(sql)
            print id(self._conn),"save"
            logging.info("sql save "+str(count))
        except Exception,e:
            logging.exception("Exception Logged")
            logging.error("sql execute error")
        finally:
            logging.info("mysql close......")
            self.dispose()

        if count==0:
            return False
        else:
            return True


    def getAll(self,sql,param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchall()
        else:
            result = False
        return result

    def getOne(self,sql,param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchone()
        else:
            result = False
        return result

    def getMany(self,sql,num,param=None):
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
            count = self._cursor.execute(sql,param)
        if count>0:
            result = self._cursor.fetchmany(num)
        else:
            result = False
        return result

    def insertOne(self,sql,value):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        self._cursor.execute(sql,value)
        return self.__getInsertId()

    def insertMany(self,sql,values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        count = self._cursor.executemany(sql,values)
        return count

    def __getInsertId(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        self._cursor.execute("SELECT @@IDENTITY AS id")
        result = self._cursor.fetchall()
        return result[0]['id']

    def __query(self,sql,param=None):
        if param is None:
            count = self._cursor.execute(sql)
        else:
            count = self._cursor.execute(sql,param)
        return count

    def update(self,sql,param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql,param)

    def delete(self,sql,param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql,param)

    def begin(self):
        """
        @summary: 开启事务
        """
        self._conn.autocommit(0)

    def end(self,option='commit'):
        """
        @summary: 结束事务
        """
        if option=='commit':
            print id(self._conn),"end"
            self._conn.commit()
        else:
            self._conn.rollback()

    def dispose(self,isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd==1:
            self.end('commit')
        else:
            self.end('rollback');
        self._cursor.close()
        self._conn.close()


class MySQLDBUtils(object):
    """
    MYSQL数据库操作类
    """

    def __init__(self):
        """
        数据库连接
        :param db_conn_dict:数据库连接参数
        :return:
        """
        self.pool = MySQLDBUtils.create_pool()
        self.pool_dict = MySQLDBUtils.create_pool(cursor_class="DictCursor")

    @gen.coroutine
    def get_all(self, query, param=None, cursor='list'):
        """
        执行查询，并取出所有结果集
        :param query:  查询语句
        :param param:  参数列表 条件列表值（元组/列表）
        :param cursor:  指定cursor的返回类型
        :return: ((column_1, column_2), (column_1, column_2))
        """
        if cursor == 'dict':
            if param:
                cursor = yield self.pool_dict.execute(query, param)
            else:
                cursor = yield self.pool_dict.execute(query)
        elif cursor == 'list':
            if param:
                cursor = yield self.pool.execute(query, param)
            else:
                cursor = yield self.pool.execute(query)
        res = cursor.fetchall()
        raise gen.Return(res)

    @gen.coroutine
    def get_one(self, query, param=None, cursor=''):
        """
        执行查询，并取出第一条
        :param query:  查询语句
        :param param:  参数列表 条件列表值（元组/列表）
        :param cursor:
        :return: (column_1, column_2, column_3)
        """
        if cursor == 'dict':
            if param:
                cursor = yield self.pool_dict.execute(query, param)
            else:
                cursor = yield self.pool_dict.execute(query)
        else:
            if param:
                cursor = yield self.pool.execute(query, param)
            else:
                cursor = yield self.pool.execute(query)
        res = cursor.fetchone()
        raise gen.Return(res)

    @gen.coroutine
    def get_many(self, query, param=None, num=20, cursor='list'):
        """
        执行查询，并取出num条结果
        :param query:  查询语句
        :param param: 可选参数，条件列表值（元组/列表）
        :param num: 指定条数
        :param cursor:
        :return: ((column_1, column_2), (column_1, column_2))
        """
        if cursor == 'dict':
            if param:
                cursor = yield self.pool_dict.execute(query, param)
            else:
                cursor = yield self.pool_dict.execute(query)
        elif cursor == 'list':
            if param:
                cursor = yield self.pool.execute(query, param)
            else:
                cursor = yield self.pool.execute(query)
        res = cursor.fetchmany(num)
        raise gen.Return(res)

    @gen.coroutine
    def insert_one(self, query, param=None):
        """
        向数据表插入一条记录
        :param query:  SQL语句
        :param param: 参数
        :return:
        """
        if param:
            res = yield self.pool.execute(query, param)
        else:
            res = yield self.pool.execute(query)
        raise gen.Return(res)

    @gen.coroutine
    def insert_many(self, query, param=None):
        """
        向数据表插入多条记录
        :param query: SQL语句
        :param param: 参数
        :return:
        """
        if param:
            res = yield self.pool.executemany(query, param)
        else:
            res = yield self.pool.executemany(query)

        raise gen.Return(res)

    @gen.coroutine
    def delete(self, query, param=None):
        """
        删除数据表记录
        :param query:  SQL语句
        :param param: 参数
        :return:
        """
        if param:
            res = yield self.pool.execute(query, param)
        else:
            res = yield self.pool.execute(query)
        raise gen.Return(res)

    @gen.coroutine
    def update(self, query, param=None):
        """
        更新数据表记录
        :param query: SQL语句
        :param param: 参数
        :return:
        """
        if param:
            res = yield self.pool.execute(query, param)
        else:
            res = yield self.pool.execute(query)
        raise gen.Return(res)

    @gen.coroutine
    def get_data_from_db(self, query, param=None, search_type='all', num=10, cursor='list'):
        """
        获取数据表的数据
        :param query: 查询语句
        :param param: 参数
        :param search_type: one:查询单条 all:查询全部 many:查询指定条数
        :param num: 指定条数
        :param cursor:
        :return:
            one: (column_1, column_2, column_3)
            all: ((column_1, column_2), (column_1, column_2))
            many: ((column_1, column_2), (column_1, column_2))
        """
        if cursor == 'dict':
            if param:
                cursor = yield self.pool_dict.execute(query, param)
            else:
                cursor = yield self.pool_dict.execute(query)
        elif cursor == 'list':
            if param:
                cursor = yield self.pool.execute(query, param)
            else:
                cursor = yield self.pool.execute(query)
        if search_type == 'one':
            res = cursor.fetchone()
        elif search_type == 'all':
            res = cursor.fetchall()
        else:
            res = cursor.fetchmany(num)
        raise gen.Return(res)

    @gen.coroutine
    def operate_db(self, query, param=None):
        """
        操作数据表的数据
        :param query: SQL语句
        :param param: 参数
        :return:
        """
        if param:
            res = yield self.pool.execute(query, param)
        else:
            res = yield self.pool.execute(query)
        raise gen.Return(res)

    @staticmethod
    def create_pool(cursor_class=''):
        """
        生成数据库连接池
        :param para: 数据库配置参数
        :param cursor_class:
        :return:
        """
        thePath = sys.path[0]
        thePath = thePath[:thePath.find("crawlerData")+11]
        CONF_MYSQL = thePath+"/resources/conf/Mysql.conf"
        cf = ConfigParser.ConfigParser()
        cf.read(CONF_MYSQL)
        if cursor_class == "DictCursor":
            return pools.Pool(dict(host=cf.get('mysqldb','host'),
                                   port=int(cf.get('mysqldb','port')),
                                   user=cf.get('mysqldb','user'),
                                   passwd=cf.get('mysqldb','passwd'),
                                   db=cf.get('mysqldb','db'),
                                   charset=cf.get('mysqldb','charset'),
                                   cursorclass=DictCursor
                                   ),
                              max_idle_connections=10,
                              max_open_connections=100,
                              max_recycle_sec=3)
        else:
            return pools.Pool(dict(host=cf.get('mysqldb','host'),
                                   port=int(cf.get('mysqldb','port')),
                                   user=cf.get('mysqldb','user'),
                                   passwd=cf.get('mysqldb','passwd'),
                                   db=cf.get('mysqldb','db'),
                                   charset=cf.get('mysqldb','charset')
                                   ),
                              max_idle_connections=10,
                              max_open_connections=100,
                              max_recycle_sec=3)



if __name__ == '__main__':
    #mysql1=Mysql()
    #mysql2=Mysql()
    #print id(mysql1)
    #print id(mysql2)
    #result=mysql1.getAll("select * from user")

    result=MySQLDBUtils().get_all("select * from user")
    print result