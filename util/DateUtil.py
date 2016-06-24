# -*- coding: utf-8 -*-
import time
import functools
import logging


class DateUtil:
    @staticmethod
    def time_me(info="used"):
        def _time_me(fn):
            @functools.wraps(fn)
            def _wrapper(*args, **kwargs):
                start = time.clock()
                result = fn(*args, **kwargs)
                logging.info('%s %s %s %s', fn.__name__, info, time.clock() - start, "second")
                return result

            return _wrapper

        return _time_me
