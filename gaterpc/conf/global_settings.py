# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/5/10 16:47
"""Gate rpc constants"""
from logging import getLogger
from typing import Any, Optional, SupportsInt


logger = getLogger("gate-rpc")


class DefaultSettings(object):
    # Base
    DEBUG = False
    RUN_PATH: str = "/var/run/gate-rpc/"
    WINDOWS_MAX_WORKERS: int = 63 - 2
    CLIENT_TIMEOUT: int = 2  # second
    WORKER_CONF: dict = {
        "io": {
            "workers_url": "inproc://io_worker",
        },
        "cpu": {
            "workers_url": f"ipc://{RUN_PATH}cpu_worker",
        }
    }
    ZMQ_HWM: int = 1000
    # ZAP
    ZAP_VERSION: bytes = b"1.0"
    ZAP_DEFAULT_DOMAIN: bytes = b"gate"
    ZAP_MECHANISM_NULL: bytes = b"NULL"
    ZAP_MECHANISM_PLAIN: bytes = b"PLAIN"
    ZAP_MECHANISM_CURVE: bytes = b"CURVE"
    ZAP_PLAIN_DEFAULT_USER: bytes = "堡垒".encode("utf-8")
    ZAP_PLAIN_DEFAULT_PASSWORD: bytes = "小鸡炖蘑菇".encode("utf-8")
    ZAP_INPROC_ADDR: str = "inproc://zeromq.zap.01"
    # MDP
    MDP_HEARTBEAT: float = 0.5
    MDP_DEFAULT_SERVICE_NAME: str = "gate"
    MDP_VERSION: str = "01"
    MDP_CLIENT: bytes = f"MDPC{MDP_VERSION}".encode("utf-8")
    MDP_WORKER: bytes = f"MDPW{MDP_VERSION}".encode("utf-8")
    MDP_COMMAND_READY: bytes = b"\x01"
    MDP_COMMAND_REQUEST: bytes = b"\x02"
    MDP_COMMAND_REPLY: bytes = b"\x03"
    MDP_COMMAND_HEARTBEAT: bytes = b"\x04"
    MDP_COMMAND_DISCONNECT: bytes = b"\x05"
    MESSAGE_QUEUE_MAX: int = 0
    # RPC
    RPC_REPLY_TEMPLATE: dict = {
        "result": Any,
        "exception": Optional[Exception]
    }
    TASK_TIMEOUT: int = 3  # second

    def __init__(self, **options):
        for name, value in options.items():
            if not name.isupper():
                logger.warning(f"Settings {name} must be uppercase.")
                continue
            setattr(self, name, value)


Settings = DefaultSettings()
# TODO: 可以使用用户配置覆盖
