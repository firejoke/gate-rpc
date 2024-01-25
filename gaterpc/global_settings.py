# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/10 16:47
"""Gate rpc constants"""
from logging.config import dictConfig
from pathlib import Path


class DefaultSettings(object):
    """
    全局配置，运行Worker、AMajorodomo、Client之前都需要先执行该配置类的setup函数
    """
    # Base
    DEBUG = False
    EVENT_LOOP_POLICY = None
    RUN_PATH: str = "/var/run/gate-rpc/"
    WINDOWS_MAX_WORKERS: int = 63 - 2
    CLIENT_TIMEOUT: int = 2  # second
    WORKER_ADDR: str = f"ipc://{RUN_PATH}cpu_worker"
    ZMQ_HWM: int = 1000
    # ZAP
    ZAP_VERSION: bytes = b"1.0"
    ZAP_DEFAULT_DOMAIN: str = "gate"
    ZAP_MECHANISM_NULL: bytes = b"NULL"
    ZAP_MECHANISM_PLAIN: bytes = b"PLAIN"
    ZAP_MECHANISM_CURVE: bytes = b"CURVE"
    ZAP_PLAIN_DEFAULT_USER: str = "堡垒"
    ZAP_PLAIN_DEFAULT_PASSWORD: str = "哔哔哔哔哔"
    ZAP_ADDR: str = f"ipc://{RUN_PATH}zeromq.zap.01"
    ZAP_REPLY_TIMEOUT: float = 5.0
    # MDP
    MDP_HEARTBEAT_INTERVAL: int = 1500
    MDP_HEARTBEAT_LIVENESS: int = 3
    MDP_INTERNAL_SERVICE_PREFIX: bytes = b"gate."
    MDP_VERSION: str = "01"
    MDP_CLIENT: bytes = f"MDPC{MDP_VERSION}".encode("utf-8")
    MDP_WORKER: bytes = f"MDPW{MDP_VERSION}".encode("utf-8")
    MDP_COMMAND_READY: bytes = b"\x01"
    MDP_COMMAND_REQUEST: bytes = b"\x02"
    MDP_COMMAND_REPLY: bytes = b"\x03"
    MDP_COMMAND_HEARTBEAT: bytes = b"\x04"
    MDP_COMMAND_DISCONNECT: bytes = b"\x05"
    # RPC
    MESSAGE_MAX: int = 1000  # 参考ZMQ HWM
    STREAM_REPLY_MAXSIZE = 0
    STREAM_GENERATOR_TAG = b"GateStreamGenerator"
    STREAM_HUGE_DATA_TAG = b"GateStreamHugData"
    STREAM_END_MESSAGE = b"GateStreamEnd"
    HUGE_DATA_SIZEOF = 1000  # MTU 1500 减去20字节ip头部，20字节tcp头部，去掉MDP的前几帧
    HUGE_DATA_COMPRESS_MODULE: str = "gzip"
    HUGE_DATA_COMPRESS_LEVEL: int = 9
    SERVICE_DEFAULT_NAME: str = "gate-rpc"
    REPLY_TIMEOUT: float = 60.0
    TASK_TIMEOUT: float = 30.0  # second
    LOG_PATH = Path("/tmp/gate-rpc/")
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
                "filename": LOG_PATH / "asyncio.log",
                "formatter": "debug",
                "when": "midnight",
                "backupCount": 10
            },
            "gaterpc": {
                "level": "DEBUG",
                "class": "gaterpc.utils.AQueueHandler",
                "handler_class": "logging.handlers.TimedRotatingFileHandler",
                "loop": "asyncio.get_running_loop",
                "filename": LOG_PATH / "gaterpc.log",
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

    def setup(self, **options):
        for name, value in options.items():
            if not name.isupper():
                Warning(f"Settings {name} must be uppercase.")
                continue
            setattr(self, name, value)
        if self.DEBUG:
            for handler in self.LOGGING["handlers"].values():
                handler["formatter"] = "debug"
            for logger in self.LOGGING["loggers"].values():
                logger["level"] = "DEBUG"
                logger["handlers"].append("console")
        self.LOG_PATH.mkdir(parents=True, exist_ok=True)
        dictConfig(self.LOGGING)
        if self.EVENT_LOOP_POLICY:
            import asyncio

            asyncio.set_event_loop_policy(self.EVENT_LOOP_POLICY)
        (run_path := Path(self.RUN_PATH)).mkdir(parents=True, exist_ok=True)


Settings = DefaultSettings()
