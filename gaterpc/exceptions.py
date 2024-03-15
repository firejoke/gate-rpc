# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/8/10 17:38
from gettext import gettext as _


class ServiceUnAvailableError(Exception):
    def __init__(self, name):
        super().__init__(
            _("The {name} Service unavailable.").format(name=name),
        )


class BuysWorkersError(Exception):
    def __init__(self):
        super().__init__(_("No idle workers."))


class RemoteException(Exception):
    def __init__(self, except_info):
        if isinstance(except_info, str):
            message = except_info
        elif isinstance(except_info, (tuple, list)):
            except_class, except_value, except_traceback = except_info
            message = (f"\n"
                       f"  RemoteTraceback ({except_class}):\n"
                       f"  {except_value}\n"
                       f"  {except_traceback}")
        else:
            raise TypeError("except_info must be str or tuple.")
        super().__init__(message)


class BadGzip(OSError):
    pass


class HugeDataException(OSError):
    pass


class DictFull(Exception):
    def __init__(self, maxsize):
        message = (f"The number of keys in the dict reached the maximum of "
                   f"{maxsize}.")
        super().__init__(message)
