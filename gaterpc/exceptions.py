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
    def __init__(self, except_class, except_value, except_traceback: list):
        message = (f"\n"
                   f"  RemoteTraceback ({except_class}):\n"
                   f"  {except_value}\n"
                   f"  {except_traceback}")
        super().__init__(message)
