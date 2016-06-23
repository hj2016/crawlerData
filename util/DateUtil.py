# -*- coding: utf-8 -*-
import time
import functools


class DateUtil:
    @staticmethod
    def time_me(info="used"):
        def _time_me(fn):
            @functools.wraps(fn)
            def _wrapper(*args, **kwargs):
                start = time.clock()
                fn(*args, **kwargs)
                print "%s %s %s" % (fn.__name__, info, time.clock() - start), "second"

            return _wrapper

        return _time_me
