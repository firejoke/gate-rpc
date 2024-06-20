# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/8/10 17:38
class ServiceUnAvailableError(Exception):
    def __init__(self, name):
        super().__init__(
            "The {name} Service unavailable.".format(name=name),
        )


class GateUnAvailableError(ServiceUnAvailableError):
    def __init__(self):
        super().__init__("Gate")


class BusyWorkerError(Exception):
    def __init__(self):
        super().__init__("No idle worker.")


class BusyGateError(Exception):
    def __init__(self):
        super().__init__("No idle gate.")


class RemoteException(Exception):
    def __init__(self, except_info):
        if isinstance(except_info, str):
            message = except_info
        elif isinstance(except_info, (tuple, list)):
            except_class, except_value, except_traceback = except_info
            message = (
                "\n"
                "  RemoteTraceback ({except_class}):\n"
                "  {except_value}\n"
                "  {except_traceback}"
            ).format(
                except_class=except_class,
                except_value=except_value,
                except_traceback=except_traceback
            )
        else:
            raise TypeError("except_info must be str or tuple.")
        super().__init__(message)


class BadGzip(OSError):
    """"""


class HugeDataException(OSError):
    """"""


class DictFull(Exception):
    def __init__(self, maxsize):
        message = (
            "The number of keys in the dict reached the maximum of {maxsize}."
        ).format(maxsize=maxsize)
        super().__init__(message)
