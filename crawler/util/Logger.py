# -*- coding: utf-8 -*-
import logging
import logging.config
import sys
import os


class Logger():
    @staticmethod
    def initLogger():
        thePath = os.path.dirname(__file__)
        thePath = thePath[:thePath.find("crawler/") + 8]
        CONF_LOG = thePath + "resources/conf/Log.conf"
        print CONF_LOG
        # 采用配置文件
        logging.config.fileConfig(CONF_LOG)
        logger = logging.getLogger("xzs")
        return logging.basicConfig(level=logging.DEBUG,
                                   format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                                   datefmt='%a, %d %b %Y %H:%M:%S',
                                   filename='myapp.log',
                                   filemode='w')
