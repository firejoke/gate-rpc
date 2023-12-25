# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/8/10 17:38

class ServiceUnAvailableError(Exception):
    def __init__(self, name):
        super().__init__(f"The {name} Service unavailable.")


class BuysWorkersError(Exception):
    def __init__(self):
        super().__init__(f"No idle workers.")
