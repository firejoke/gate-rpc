# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/10 16:47
"""Gate rpc constants"""
import asyncio
import os
from importlib import import_module
from logging.config import dictConfig
from pathlib import Path
import zmq.constants as z_const

from .utils import (
    TypeValidator, LazyAttribute, UnixEPollEventLoopPolicy,
    empty, ensure_mkdir,
)


def user_settings_render(settings: "GlobalSettings", user_settings):
    if isinstance(user_settings, str):
        return import_module(user_settings)
    elif user_settings is empty:
        return None
    return user_settings


def logging_render(settings: "GlobalSettings", logging: dict):
    if logging is empty:
        return dict()
    for name, handler in logging["handlers"].items():
        if handler.get("filename") is empty:
            handler["filename"] = settings.LOG_PATH.joinpath(f"{name}.log")
    return logging


def logging_process(settings: "GlobalSettings", logging: dict):
    logging = TypeValidator(dict)(logging)
    return logging


_DEFAULT_LOGGING = {
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
            "listener": "gaterpc.utils.singleton_aqueue_listener",
            "filename": empty,
            "formatter": "debug",
            "when": "midnight",
            "backupCount": 10
        },
        "gaterpc": {
            "level": "DEBUG",
            "class": "gaterpc.utils.AQueueHandler",
            "handler_class": "logging.handlers.TimedRotatingFileHandler",
            "listener": "gaterpc.utils.singleton_aqueue_listener",
            "filename": empty,
            "formatter": "verbose",
            "when": "midnight",
            "backupCount": 10
        },
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
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
            "propagate": False,
        },
        "commands": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
    },
}


class GlobalSettings(object):
    """
    全局配置，运行Worker、AMajorodomo、Client之前都需要先执行该配置类的setup函数
    """
    # system
    DEBUG = False
    ENVIRONMENT = LazyAttribute(
        raw=dict(),
        process=lambda instance, p: TypeValidator(dict)(p)
    )

    # Base
    BASE_PATH: Path = LazyAttribute(
        Path("/tmp/gate-rpc/"),
        render=lambda instance, p: ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    RUN_PATH: Path = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.BASE_PATH.joinpath("run/")
        ) if p is empty else ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    LOG_PATH: Path = LazyAttribute(
        render=lambda instance, p: ensure_mkdir(
            instance.BASE_PATH.joinpath("log/")
        ) if p is empty else ensure_mkdir(p),
        process=lambda instance, p: TypeValidator(Path)(p)
    )
    HEARTBEAT: int = 5000  # millisecond
    TIMEOUT: float = 30.0
    WINDOWS_MAX_WORKERS: int = 63 - 2
    WORKER_ADDR: str = "ipc:///tmp/gate-rpc/w1"
    USER_SETTINGS = LazyAttribute(render=user_settings_render)
    SECURE = True
    # ZMQ
    ZMQ_CONTEXT = {
        z_const.IPV6: 1
    }
    ZMQ_SOCK = {
        z_const.HWM: 3000,
        # millisecond
        # z_const.SNDTIMEO: 10 * 1000,
        # millisecond
        # z_const.RCVTIMEO: 10 * 1000
    }
    # ZAP
    ZAP_VERSION: bytes = b"1.0"
    ZAP_DEFAULT_DOMAIN: str = "gate"
    ZAP_MECHANISM_NULL: bytes = b"NULL"
    ZAP_MECHANISM_PLAIN: bytes = b"PLAIN"
    ZAP_MECHANISM_CURVE: bytes = b"CURVE"
    ZAP_MECHANISM_GSSAPI: bytes = b"GSSAPI"
    ZAP_PLAIN_DEFAULT_USER: str = "堡垒"
    ZAP_PLAIN_DEFAULT_PASSWORD: str = "哔哔哔哔哔"
    ZAP_ADDR: str = LazyAttribute(
        render=lambda instance, p:
        f"ipc://{instance.RUN_PATH.joinpath('gate.zap').as_posix()}"
        if p is empty else p
    )
    ZAP_REPLY_TIMEOUT: float = 5.0
    # MDP
    MDP_HEARTBEAT_INTERVAL: int = LazyAttribute(
        render=lambda instance, p: instance.HEARTBEAT if p is empty else p
    )  # # millisecond
    MDP_HEARTBEAT_LIVENESS: int = 3
    MDP_INTERNAL_SERVICE: str = "Gate"
    MDP_VERSION: str = "01"
    MDP_CLIENT: bytes = LazyAttribute(
        render=lambda instance, p: f"MDPC{instance.MDP_VERSION}".encode("utf-8")
    )
    MDP_WORKER: bytes = LazyAttribute(
        render=lambda instance, p: f"MDPW{instance.MDP_VERSION}".encode("utf-8")
    )
    MDP_COMMAND_READY: bytes = b"\x01"
    MDP_COMMAND_REQUEST: bytes = b"\x02"
    MDP_COMMAND_REPLY: bytes = b"\x03"
    MDP_COMMAND_HEARTBEAT: bytes = b"\x04"
    MDP_COMMAND_DISCONNECT: bytes = b"\x05"
    MDP_DESCRIPTION_SEP: str = ":"
    # Gate
    GATE_CLUSTER_NAME: str = "GateCluster"
    GATE_CLUSTER_DESCRIPTION: str = "GateRPC cluster service"

    GATE_VERSION: str = "01"
    GATE_MEMBER: bytes = LazyAttribute(
        render=lambda instance, p:
        f"GATEM{instance.GATE_VERSION}".encode("utf-8")
    )
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
    HUGE_DATA_COMPRESS_LEVEL: int = 1
    HUGE_DATA_END_TAG = b"HugeDataEND"
    HUGE_DATA_EXCEPT_TAG = b"HugeDataException"
    SERVICE_DEFAULT_NAME: str = "GateRPC"
    REPLY_TIMEOUT: float = LazyAttribute(
        render=lambda instance, p: 2 * instance.TIMEOUT if p is empty else p
    )
    # log
    DEFAULT_LOGGING = LazyAttribute(
        _DEFAULT_LOGGING,
        render=logging_render, process=logging_process
    )
    LOGGING = LazyAttribute(render=logging_render, process=logging_process)

    def configure(self, name, value):
        if isinstance(value, LazyAttribute):
            setattr(GlobalSettings, name, value)
        else:
            setattr(self, name, value)

    def setup(self, **options):
        if self.USER_SETTINGS:
            for name in dir(self.USER_SETTINGS):
                if name.isupper() and not name.startswith("_"):
                    value = getattr(self.USER_SETTINGS, name)
                    self.configure(name, value)
        for name, value in options.items():
            if not name.isupper():
                Warning(f"Settings {name} must be uppercase.")
                continue
            self.configure(name, value)

        if self.DEBUG:
            self.ENVIRONMENT["PYTHONASYNCIODEBUG"] = "1"

        for name, value in self.ENVIRONMENT.items():
            os.environ[name] = value

        dictConfig(self.DEFAULT_LOGGING)
        if self.LOGGING:
            dictConfig(self.LOGGING)


Settings = GlobalSettings()
