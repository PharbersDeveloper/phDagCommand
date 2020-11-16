# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import io
import sys
import time
import base64
import atexit
import logging
from define_value import *
from ph_aws.ph_s3 import PhS3
from ph_aws.ph_sts import PhSts

LOG_LEVEL = logging.DEBUG #ERROR
LOG_PATH = '{}/logs/python/phcli/{}'


class PhLogs(object):
    """The Pharbers Logs
    """

    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger("ph-log")
        self.logger.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter("{ 'Time': %(asctime)s, 'Message': %(message)s, 'File': %(filename)s, 'Func': "
                                      "%(funcName)s, 'Line': %(lineno)s, 'Level': %(levelname)s } ")

        sys_handler = logging.StreamHandler(stream=sys.stdout)
        sys_handler.setFormatter(formatter)
        self.logger.addHandler(sys_handler)

        if 'job_id' not in kwargs.keys() or not kwargs['job_id']:
            self._log_path = LOG_PATH.format(CLI_VERSION, 'null_id_' + str(time.time()) + '.log')
        else:
            self._log_path = LOG_PATH.format(CLI_VERSION, kwargs['job_id'] + '.log')

        def write_s3_logs(body, bucket, key):
            phsts = PhSts().assume_role(
                base64.b64decode(ASSUME_ROLE_ARN).decode(),
                ASSUME_ROLE_EXTERNAL_ID,
            )
            phs3 = PhS3(phsts=phsts)
            phs3.s3_client.put_object(Body=body.getvalue(), Bucket=bucket, Key=key)

        if 'storage' in kwargs.keys() and kwargs['storage'] == 's3':
            log_stream = io.StringIO()
            io_handler = logging.StreamHandler(log_stream)
            io_handler.setFormatter(formatter)
            for handler in self.logger.handlers:
                self.logger.removeHandler(handler)
            self.logger.addHandler(io_handler)
            atexit.register(write_s3_logs, body=log_stream, bucket=CLI_BUCKET, key=self._log_path)

phlogger = PhLogs().logger
phs3logger = lambda job_id: PhLogs(job_id=job_id, storage='s3').logger

if __name__ == '__main__':
    phlogger.debug('debug')
    phlogger.info('info')
    phlogger.warning('warning')
    phlogger.error('error')
    phlogger.critical('critical')

    joblog = phs3logger('job')
    joblog.debug('debug')
    joblog.info('info')
    joblog.warning('warning')
    joblog.error('error')
    joblog.critical('critical')

