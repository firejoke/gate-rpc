# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/14 10:57
from pathlib import Path

import zmq.constants as z_const
from gaterpc.utils import empty


DEBUG = 1
SECURE = True

ZMQ_SOCK = {
    z_const.SocketOption.HWM: 5000,
    # millisecond
    # z_const.SNDTIMEO: 10 * 1000,
    # millisecond
    # z_const.RCVTIMEO: 10 * 1000
}
# WORKER_ADDR: str = "tcp://0:777"
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
