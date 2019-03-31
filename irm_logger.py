#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""

@version: 0.1??
@author:  quantpy
@email:   quantpy@qq.com
@file:    logger.py
@time:    2017-05-26 13:45
"""


import logging
import tqdm


class TqdmLoggingHandler (logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super(self.__class__, self).__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class Logger(logging.getLoggerClass()):
    def __init__(self, name='', level=logging.WARNING, handler=None):
        super(Logger, self).__init__(name)
        if handler is None:
            # handler = logging.StreamHandler()
            # handler = TqdmLoggingHandler()
            handler = logging.FileHandler('./limit_up_engine.log', mode='a', encoding=None, delay=False)

        formatter = logging.Formatter("%(levelname)s[%(lineno)3d]:: %(message)s--------%(asctime)s")
        # formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
        handler.setFormatter(formatter)
        self.addHandler(handler)
        self.setLevel(level)

    def warning(self, msg, *args, **kwargs):
        super(Logger, self).warning(repr(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        super(Logger, self).error(repr(msg), *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        super(Logger, self).debug(repr(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        super(Logger, self).info(repr(msg), *args, **kwargs)



