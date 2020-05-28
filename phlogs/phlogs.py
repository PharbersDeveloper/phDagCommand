# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import logging
import sys


class PhLogs(object):
    """The Pharbers Logs
    """

    def __init__(self):
        self.logger = logging.getLogger("ph-log")
        self.logger.setLevel(level=logging.INFO)
        handler = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter(" { 'Time': %(asctime)s, 'Message': %(message)s, 'File': %(filename)s, 'Func': %(funcName)s, 'Line': %(lineno)s, 'Level': %(levelname)s } ")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)


phlogger = PhLogs().logger
