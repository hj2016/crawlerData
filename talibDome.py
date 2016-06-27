# -*- coding: UTF-8 -*-
import time

import talib

import numpy
from util.DateUtil import DateUtil


class test():

    @DateUtil.time_me()
    def testfun(self):
        close = numpy.random.random(100)
        output = talib.SMA(close)
        print close
        print output
        time.sleep(0.1)


if __name__ == '__main__':
    t=test()
    t.testfun()