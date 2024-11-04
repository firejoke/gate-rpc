# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2024/8/14 10:57
import sys
from pathlib import Path

import zmq.constants as z_const


base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.utils import empty


DEBUG = 0
SECURE = True

ZMQ_SOCK = {
    z_const.SocketOption.IPV6: 1,
    z_const.SocketOption.HWM: 7000,
    # millisecond
    # z_const.SNDTIMEO: 10 * 1000,
    # millisecond
    # z_const.RCVTIMEO: 10 * 1000
}
# WORKER_ADDR: str = "tcp://0:777"

MESSAGE_MAX: int = 30000
# GATE_IP_VERSION: int = 6
# GATE_MULTICAST_GROUP: str = "ff08::7"
# GATE_MULTICAST_PORT: int = 9777
# GATE_MULTICAST_HOP_LIMIT: int = 2

debug_format = (
    "%(asctime)s %(name)s "
    "[%(processName)s(%(process)d):%(threadName)s(%(thread)d)]\n"
    "%(pathname)s[%(funcName)s:%(lineno)d] \n"
    "- %(levelname)s %(message)s"
)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "debug": {
            "()": "gaterpc.utils.ColorFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "[%(processName)s(%(process)d):"
                      "%(threadName)s(%(thread)d)]\n"
                      "%(pathname)s[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
        "verbose": {
            "()": "gaterpc.utils.ColorFormatter",
            "format": "%(asctime)s %(levelname)s %(name)s "
                      "%(module)s.[%(funcName)s:%(lineno)d] "
                      "-\n%(message)s",
        },
    },
    "handlers": {
        "test_gate": {
            "level": "DEBUG",
            "class": "gaterpc.utils.AQueueHandler",
            "handler_class": "logging.handlers.TimedRotatingFileHandler",
            "listener": "gaterpc.utils.singleton_aqueue_listener",
            "filename": empty,
            "formatter": "debug",
            "when": "midnight",
            "backupCount": 10
        },
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "debug",
        },
    },
    "loggers": {
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
        "gaterpc.worker": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": False,
        },
        "gaterpc.client": {
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
