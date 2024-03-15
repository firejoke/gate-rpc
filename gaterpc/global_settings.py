# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/10 16:47
"""Gate rpc constants"""
from collections import deque
from logging.config import dictConfig
from pathlib import Path
from random import randint
from typing import Union

from gaterpc.utils import _LazyProperty


class DefaultSettings(object):
    """
    全局配置，运行Worker、AMajorodomo、Client之前都需要先执行该配置类的setup函数
    """
    # Base
    DEBUG = False
    BASE_PATH = Path("/tmp/gate-rpc/")
    HEARTBEAT: int = 3000  # millisecond
    TIMEOUT: float = 30.0
    EVENT_LOOP_POLICY = None
    WINDOWS_MAX_WORKERS: int = 63 - 2
    WORKER_ADDR: str = f"inproc://gate.worker.01"
    GATE_CLUSTER_NAME: str = "GateCluster"
    GATE_CLUSTER_DESCRIPTION: str = "GateRPC cluster service"
    # ZMQ
    ZMQ_CONTEXT_IPV6: int = 1
    ZMQ_SOCK_HWM: int = 3000
    # ZMQ_SOCK_SNDTIMEO: int = 10 * 1000  # millisecond
    # ZMQ_SOCK_RCVTIMEO: int = 10 * 1000  # millisecond
    # ZAP
    ZAP_VERSION: bytes = b"1.0"
    ZAP_DEFAULT_DOMAIN: str = "gate"
    ZAP_MECHANISM_NULL: bytes = b"NULL"
    ZAP_MECHANISM_PLAIN: bytes = b"PLAIN"
    ZAP_MECHANISM_CURVE: bytes = b"CURVE"
    ZAP_MECHANISM_GSSAPI: bytes = b"GSSAPI"
    ZAP_PLAIN_DEFAULT_USER: str = "堡垒"
    ZAP_PLAIN_DEFAULT_PASSWORD: str = "哔哔哔哔哔"
    ZAP_ADDR: str = _LazyProperty(
        lambda ins:
        f"ipc://{_LazyProperty('RUN_PATH')(ins)}gate.zap.{randint(1, 10)}"
    )
    ZAP_REPLY_TIMEOUT: float = 5.0
    # MDP
    MDP_HEARTBEAT_INTERVAL: int = _LazyProperty("HEARTBEAT")  # millisecond
    MDP_HEARTBEAT_LIVENESS: int = 3
    MDP_INTERNAL_SERVICE: str = "Gate"
    MDP_VERSION: str = "01"
    MDP_CLIENT: bytes = f"MDPC{MDP_VERSION}".encode("utf-8")
    MDP_WORKER: bytes = f"MDPW{MDP_VERSION}".encode("utf-8")
    MDP_COMMAND_READY: bytes = b"\x01"
    MDP_COMMAND_REQUEST: bytes = b"\x02"
    MDP_COMMAND_REPLY: bytes = b"\x03"
    MDP_COMMAND_HEARTBEAT: bytes = b"\x04"
    MDP_COMMAND_DISCONNECT: bytes = b"\x05"
    # Gate
    GATE_VERSION: str = "01"
    GATE_MEMBER: bytes = f"GATEM{GATE_VERSION}".encode("utf-8")
    GATE_COMMAND_RING: bytes = b"\x00"
    GATE_COMMAND_READY: bytes = b"\x01"
    GATE_COMMAND_REQUEST: bytes = b"\x02"
    GATE_COMMAND_REPLY: bytes = b"\x03"
    GATE_COMMAND_HEARTBEAT: bytes = b"\x04"
    GATE_COMMAND_DISCONNECT: bytes = b"\x05"
    # RPC
    MESSAGE_MAX: int = 5000  # 参考ZMQ HWM
    STREAM_REPLY_MAXSIZE = 0
    STREAM_GENERATOR_TAG = b"GateStreamGenerator"
    STREAM_EXCEPT_TAG = b"GateStreamException"
    STREAM_END_TAG = b"GateStreamEnd"
    STREAM_HUGE_DATA_TAG = b"GateStreamHugData"
    HUGE_DATA_SIZEOF = 1000  # MTU 1500 减去20字节ip头部，20字节tcp头部，去掉MDP的前几帧
    HUGE_DATA_COMPRESS_MODULE: str = "gzip"
    HUGE_DATA_COMPRESS_LEVEL: int = 9
    HUGE_DATA_END_TAG = b"HugeDataEND"
    HUGE_DATA_EXCEPT_TAG = b"HugeDataException"
    SERVICE_DEFAULT_NAME: str = "GateRPC"
    REPLY_TIMEOUT: float = 2 * _LazyProperty("TIMEOUT")
    # log
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
                "loop": "asyncio.get_running_loop",
                "filename": "asyncio.log",
                "formatter": "debug",
                "when": "midnight",
                "backupCount": 10
            },
            "gaterpc": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.handlers.TimedRotatingFileHandler",
                "loop": "asyncio.get_running_loop",
                "filename": "gaterpc.log",
                "formatter": "verbose",
                "when": "midnight",
                "backupCount": 10
            },
            "console": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.StreamHandler",
                "loop": "asyncio.get_running_loop",
                "formatter": "simple",
            },
        },
        "loggers": {
            "multiprocessing": {
                "handlers": ["gaterpc", "console"],
                "propagate": False,
                "level": "DEBUG"
            },
            "asyncio": {
                "level": "INFO",
                "handlers": ["asyncio"],
                "propagate": False,
            },
            "gaterpc": {
                "level": "INFO",
                "handlers": ["gaterpc"],
                "propagate": True,
            },
            "gaterpc.zap": {
                "level": "INFO",
                "handlers": ["gaterpc"],
                "propagate": True,
            },
            "commands": {
                "level": "INFO",
                "handlers": ["gaterpc", "console"],
                "propagate": False,
            },
        },
    }

    def __init__(self):
        self._options = deque()
        for k in DefaultSettings.__dict__:
            if k.isupper():
                self._options.append(k)
        self.RUN_PATH = self.BASE_PATH.joinpath("run/")
        self.LOG_PATH = self.BASE_PATH.joinpath("log/")

    def __getattribute__(self, item):
        attr = super().__getattribute__(item)
        if isinstance(attr, _LazyProperty):
            return attr(self)
        return attr

    def __iter__(self):
        return iter(self._options)

    def __contains__(self, item):
        return item in self._options

    def setup(self, **options):
        for name, value in options.items():
            if not name.isupper():
                Warning(f"Settings {name} must be uppercase.")
                continue
            if name in ("RUN_PATH", "LOG_PATH") and not isinstance(value, Path):
                value = Path(value)
            setattr(self, name, value)
            self._options.append(name)
        if self.DEBUG:
            for handler in self.LOGGING["handlers"].values():
                handler["formatter"] = "debug"
            for logger in self.LOGGING["loggers"].values():
                logger["level"] = "DEBUG"
                logger["handlers"].append("console")
        self.LOG_PATH.mkdir(parents=True, exist_ok=True)
        for name, handler in self.LOGGING["handlers"].items():
            if "filename" in handler:
                handler["filename"] = self.LOG_PATH / handler["filename"]
        dictConfig(self.LOGGING)
        self.RUN_PATH.mkdir(parents=True, exist_ok=True)
        # self.ZAP_ADDR = self.RUN_PATH.joinpath(
        #     f"gate.zap.{randint(1, 10)}"
        # ).as_posix()
        # self.ZAP_ADDR = f"ipc://{self.ZAP_ADDR}"
        if self.EVENT_LOOP_POLICY:
            import asyncio

            asyncio.set_event_loop_policy(self.EVENT_LOOP_POLICY)


Settings = DefaultSettings()
