# -*- coding: UTF-8 -*-
from service import EtlService


class EtlController:
    def __init__(self):
        self.etlService = EtlService()

    def etlMktEqud(self, startDate, endDate):
        self.etlService.mktEqudService(startDate, endDate)
