# -*- coding: utf-8 -*-
__author__ = 'upsmart'

import Client
import Logger


class GetDataUtil(object):
    # 沪深股票日行情
    @staticmethod
    def getMktEqud(indexID='', ticker='', tradeDate='', beginDate='', endDate='', field=''):
        mktIdxdUrl = "/api/market/getMktEqud.csv?field=${field}&beginDate=${beginDate}&endDate=${endDate}&indexID=${indexID}&ticker=${ticker}&tradeDate=${tradeDate}"
        mktIdxdUrl = mktIdxdUrl.replace("${field}", field)
        mktIdxdUrl = mktIdxdUrl.replace("${indexID}", indexID)
        mktIdxdUrl = mktIdxdUrl.replace("${ticker}", ticker)
        mktIdxdUrl = mktIdxdUrl.replace("${tradeDate}", tradeDate)
        mktIdxdUrl = mktIdxdUrl.replace("${beginDate}", beginDate)
        mktIdxdUrl = mktIdxdUrl.replace("${endDate}", endDate)
        return Client.Client.getAPIWldata(mktIdxdUrl)

    # 证券编码及基本上市信息
    @staticmethod
    def getSecID(field="", assetClass="", ticker="", partyID="", cnSpell=""):
        secIDUrl = "/api/master/getSecID.csv?field=${field}&assetClass=${assetClass}&ticker=${ticker}&partyID=${partyID}&cnSpell=${cnSpell}"
        secIDUrl = secIDUrl.replace("${field}", field)
        secIDUrl = secIDUrl.replace("${assetClass}", assetClass)
        secIDUrl = secIDUrl.replace("${ticker}", ticker)
        secIDUrl = secIDUrl.replace("${partyID}", partyID)
        secIDUrl = secIDUrl.replace("${cnSpell}", cnSpell)
        return Client.Client.getAPIWldata(secIDUrl)

    # 指数基本信息
    @staticmethod
    def getIdx(secID="", ticker="", field=""):
        IdxUrl = "/api/idx/getIdx.csv?field=${field}&ticker=${ticker}&secID=${secID}"
        IdxUrl = IdxUrl.replace("${secID}", secID)
        IdxUrl = IdxUrl.replace("${ticker}", ticker)
        IdxUrl = IdxUrl.replace("${field}", field)
        return Client.Client.getAPIWldata(IdxUrl)

    # 指数日行情
    @staticmethod
    def getMktIdxd(indexID="", ticker="", tradeDate="", beginDate="", endDate="", field=""):
        mktIdxdUrl = "/api/market/getMktIdxd.csv?field=${field}&beginDate=${beginDate}&endDate=${endDate}&indexID=${indexID}&ticker=${ticker}&tradeDate=${tradeDate}"
        mktIdxdUrl = mktIdxdUrl.replace("${indexID}", indexID)
        mktIdxdUrl = mktIdxdUrl.replace("${ticker}", ticker)
        mktIdxdUrl = mktIdxdUrl.replace("${tradeDate}", tradeDate)
        mktIdxdUrl = mktIdxdUrl.replace("${beginDate}", beginDate)
        mktIdxdUrl = mktIdxdUrl.replace("${endDate}", endDate)
        mktIdxdUrl = mktIdxdUrl.replace("${field}", field)
        return Client.Client.getAPIWldata(mktIdxdUrl)

    # 行业分类标准
    @staticmethod
    def getIndustry(industry="", industryVersionCD="", secID="", ticker="", intoDate="", field=""):
        industryUrl = "/api/equity/getEquIndustry.csv?field=${field}&industryVersionCD=${industryVersionCD}&industry=${industry}&secID=${secID}&ticker=${ticker}&intoDate=${intoDate}"
        industryUrl = industryUrl.replace("${industry}", industry)
        industryUrl = industryUrl.replace("${industryVersionCD}", industryVersionCD)
        industryUrl = industryUrl.replace("${secID}", secID)
        industryUrl = industryUrl.replace("${ticker}", ticker)
        industryUrl = industryUrl.replace("${intoDate}", intoDate)
        industryUrl = industryUrl.replace("${field}", field)
        return Client.Client.getAPIWldata(industryUrl)

    @staticmethod
    def getXlStockInfo():
        return GetDataUtil.getXlData("hs_a")


    @staticmethod
    def getXlIndexInfo():
        return GetDataUtil.getXlData("dpzs")


    @staticmethod
    def getXlData(typeName):
        xlStockUrl = '/d/api/openapi_proxy.php/?__s=[["hq","${typeName}","",0,${page},${size}]]&type=json'
        size = 500
        page = 1
        resultStr = []
        while True:
            url = xlStockUrl.replace("${page}", str(page)).replace("${size}", str(size)).replace("${typeName}",typeName)
            result = Client.Client.getAPIXldata(url)
            count = result[0]['count']
            trade = result[0]['day']
            resultStr = resultStr + result[0]['items']
            # result[0]['items']
            if ((page * 500) > count):
                return trade,resultStr
            else:
                page = page + 1


if __name__ == '__main__':
    Logger.Logger.initLogger()
    # mktEqud = GetDataUtil.getMktEqud(beginDate='20160401', endDate='20160401', ticker='600000')
    # secID = GetDataUtil.getSecID(ticker="600000")
    # Idx = GetDataUtil.getIdx(ticker="000001")
    # mktIdxd = GetDataUtil.getMktIdxd(tradeDate="20160601")
    # industry = GetDataUtil.getIndustry(industryVersionCD="010303")
    xl = GetDataUtil.getXlStockInfo()
