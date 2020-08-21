# -*- coding: utf-8 -*-

import os


class PhLocalStorage:

    def remove(self, path):
        try:
            os.remove(path)
        except IOError:
            return False
        else:
            return True
