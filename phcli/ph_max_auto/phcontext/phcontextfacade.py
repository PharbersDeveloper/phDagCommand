# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import base64

from phcli.ph_aws.ph_s3 import PhS3
from phcli.ph_aws.ph_sts import PhSts
from phcli.ph_max_auto import define_value as dv
from phcli.ph_logs.ph_logs import phs3logger, LOG_ERROR_LEVEL
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_c9 import PhIDEC9
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_jupyter import PhIDEJupyter


ide_table = {
    'c9': PhIDEC9,
    'jupyter': PhIDEJupyter,
}


class PhContextFacade(object):
    def __init__(self, **kwargs):
        self.logger = phs3logger(level=LOG_ERROR_LEVEL)
        self.phsts = PhSts().assume_role(
            base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
            dv.ASSUME_ROLE_EXTERNAL_ID,
        )
        self.phs3 = PhS3(phsts=self.phsts)
        self.__dict__.update(kwargs)

    def command_create_exec(self):
        self.logger.debug("sub command create")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.create()

    def command_run_exec(self):
        self.logger.debug("sub command run")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.run()

    def command_combine_exec(self):
        self.logger.debug("sub command combine")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.combine()

    def command_dag_exec(self):
        self.logger.debug("sub command dag")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.dag()

    def command_publish_exec(self):
        self.logger.debug("sub command publish")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.publish()

    def command_online_run_exec(self):
        self.logger.debug("sub command online_run")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.online_run()

    def command_status_exec(self):
        self.logger.debug("sub command status")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.status()
