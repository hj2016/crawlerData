# -*- coding: UTF-8 -*-
from service.EtlService import EtlService
from util import Logger
import sys
import logging


class EtlController:
    def __init__(self):
        self.etlService = EtlService()

    def xlDataSaveJob(self):
        logging.info("start executing dayIndexData function")
        self.etlService.dayIndexData()
        logging.info("start executing dayStockData function")
        self.etlService.dayStockData()
        logging.info("start executing industryData function")
        self.etlService.industryData()
        logging.info("start executing conceptData function")
        self.etlService.conceptData()
        logging.info("start executing regionData function")
        self.etlService.regionData()

    def wlDataSavejob(self, startDate, endDate):
        logging.info("start executing mktEqudDataSave function")
        self.etlService.mktEqudDataSave(startDate, endDate)
        logging.info("start executing mktIdxdSave function")
        self.etlService.mktIdxdSave(startDate, endDate)



if __name__ == '__main__':
    Logger.Logger.initLogger()
    controller = EtlController()
    if len(sys.argv) > 1:
        if sys.argv[1] == 'xlDataSaveJob':
            controller.xlDataSaveJob()
        elif sys.argv[1] == 'wlDataSavejob':
            if len(sys.argv) == 3:
                controller.wlDataSavejob(sys.argv[2],sys.argv[3])
            else:
                logging.error("xlDataSaveJob must incoming 3 parameter,new you incoming %s parameter",len(sys.argv))
    else:
        logging.error("runing error parameter support only xlDataSaveJob and wlDataSavejob.")