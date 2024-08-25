# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/14 10:57
import zmq.constants as z_const
from gaterpc.utils import empty


DEBUG = True
SECURE = True
# EVENT_LOOP_POLICY = None

ZMQ_SOCK = {
    z_const.HWM: 3000,
    # millisecond
    # z_const.SNDTIMEO: 10 * 1000,
    # millisecond
    # z_const.RCVTIMEO: 10 * 1000
}
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "debug": {
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "[%(processName)s(%(process)d):"
                      "%(threadName)s(%(thread)d)]\n"
                      "%(pathname)s[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
        "verbose": {
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "%(module)s.[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
        "simple": {
            "format": "%(asctime)s %(levelname)s  %(name)s %(module)s "
                      "- %(message)s"
        },
    },
    "handlers": {
        "asyncio": {
            "level": "DEBUG",
            "class": "gaterpc.utils.AQueueHandler",
            "handler_class": "logging.handlers.TimedRotatingFileHandler",
            "filename": empty,
            "formatter": "debug",
            "when": "midnight",
            "backupCount": 10
        },
        "gaterpc": {
            "level": "DEBUG",
            "class": "gaterpc.utils.AQueueHandler",
            "handler_class": "logging.handlers.TimedRotatingFileHandler",
            "filename": empty,
            "formatter": "verbose",
            "when": "midnight",
            "backupCount": 10
        },
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
    },
    "loggers": {
        # "multiprocessing": {
        #     "handlers": ["console"],
        #     "propagate": False,
        #     "level": "DEBUG"
        # },
        # "asyncio": {
        #     "level": "DEBUG",
        #     "handlers": ["console"],
        #     "propagate": False,
        # },
        "gaterpc": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": True,
        },
        "gaterpc.zap": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": False,
        },
        "commands": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": False,
        },
    },
}
