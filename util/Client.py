# -*- coding: utf-8 -*-
__author__ = 'huang jing'
import httplib
import traceback
import urllib
import json
import logging
import time

HTTP_OK = 200
HTTP_AUTHORIZATION_ERROR = 401


class Client:
    XINLANG_URL = 'money.finance.sina.com.cn'
    WANLIAN_URL = 'api.wmcloud.com'
    WANLIAN_PORT = 443
    token = ''
    httpClient = None

    def __init__(self):
        self.httpClient = httplib.HTTPSConnection(self.WANLIAN_URL, self.WANLIAN_PORT)

    def __del__(self):
        if self.httpClient is not None:
            self.httpClient.close()

    def encodepath(self, path):
        # 转换参数的编码
        start = 0
        n = len(path)
        re = ''
        i = path.find('=', start)
        while i != -1:
            re += path[start:i + 1]
            start = i + 1
            i = path.find('&', start)
            if (i >= 0):
                for j in range(start, i):
                    if (path[j] > '~'):
                        re += urllib.quote(path[j])
                    else:
                        re += path[j]
                re += '&'
                start = i + 1
            else:
                for j in range(start, n):
                    if (path[j] > '~'):
                        re += urllib.quote(path[j])
                    else:
                        re += path[j]
                start = n
            i = path.find('=', start)
        return re

    def init(self, token):
        self.token = token

    def getwlData(self, path, token):
        result = None
        path = '/data/v1' + path
        path = self.encodepath(path)
        try:
            # set http header here
            self.httpClient.request('GET', path, headers={"Authorization": "Bearer " + token})

            # make request
            response = self.httpClient.getresponse()
            # read result
            if response.status == HTTP_OK:
                # parse json into python primitive object
                result = response.read()
            else:
                result = response.read()
            if (path.find('.csv?') != -1):
                result = result.decode('gbk').encode('utf-8')
            if (path.find('.json?') != -1):
                result = json.loads(result)
            return response.status, result
        except Exception, e:
            traceback.print_exc()

    def getxlData(self, path):
        path = self.encodepath(path)
        try:
            httpClient = httplib.HTTPSConnection(Client.XINLANG_URL)
            # set http header here
            httpClient.request('GET', path, headers={})
            # make request
            response = httpClient.getresponse()
            # read result
            if response.status == HTTP_OK:
                # parse json into python primitive object
                result = response.read()
            else:
                result = response.read()
            if (path.find('type=csv') != -1):
                result = result.decode('gbk').encode('utf-8')
            if (path.find('type=json') != -1):
                result = json.loads(result.decode('gbk').encode('utf-8'))
            return response.status, result
        except Exception, e:
            traceback.print_exc()

    @staticmethod
    def getAPIWldata(url):
        try:
            start = time.clock()
            logging.info("getdataUrl:" + url)
            client = Client()
            code, result = client.getwlData(url, '1a162aa9a35e6bb017abc9c672d5eacb64958895d41bdc54668db1d7e7562f30')
            end = time.clock() - start
            logging.info("api 调用用时" + str(end) + "ms")
            if code == 200:
                return result
            else:
                return code
        except Exception, e:
            logging.error(traceback.print_exc())
            raise e

    @staticmethod
    def getAPIXldata(url):
        try:
            start = time.clock()
            client = Client()
            code, result = client.getxlData(url)
            end = time.clock() - start
            logging.info("api 调用用时 %s ms", end)
            if code == 200:
                return result
            else:
                return code
        except Exception, e:
            logging.error(traceback.print_exc())
            raise e


if __name__ == "__main__":
    result = Client.getAPIWldata(
        '/api/market/getMktEqud.csv?field=&beginDate=20160401&endDate=20160401&secID=&ticker=600000&tradeDate=')
    print(result)

    result = Client.getAPIXldata('/d/api/openapi_proxy.php/?__s=[["hq","hs_a","",0,2,40]]&type=json')
    print(result)


