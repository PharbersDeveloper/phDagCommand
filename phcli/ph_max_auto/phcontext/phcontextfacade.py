# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import base64

from phcli.ph_aws.ph_s3 import PhS3
from phcli.ph_aws.ph_sts import PhSts
from phcli.ph_max_auto import define_value as dv
from phcli.ph_logs.ph_logs import phs3logger, LOG_WARN_LEVEL
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_c9 import PhIDEC9
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_jupyter import PhIDEJupyter


class PhContextFacade(object):
    def __init__(self, **kwargs):
        self.logger = phs3logger(level=LOG_WARN_LEVEL)
        self.phsts = PhSts().assume_role(
            base64.b64decode(dv.ASSUME_ROLE_ARN).decode(),
            dv.ASSUME_ROLE_EXTERNAL_ID,
        )
        self.phs3 = PhS3(phsts=self.phsts)
        self.__dict__.update(kwargs)
        self.ide_table = {
            'c9': PhIDEC9,
            'jupyter': PhIDEJupyter,
        }
        self.ide_inst = self.ide_table[self.ide](**self.__dict__)

    def command_create_exec(self):
        self.logger.debug("sub command create")
        self.ide_inst.create()

    def command_run_exec(self):
        self.logger.debug("sub command run")
        self.ide_inst.run()

    def command_combine_exec(self):
        self.logger.debug("sub command combine")
        self.ide_inst.combine()

    def command_dag_exec(self):
        self.logger.debug("sub command dag")
        self.ide_inst.dag()

    def command_publish_exec(self):
        self.logger.debug("sub command publish")
        self.ide_inst.publish()

    def command_recall_exec(self):
        self.logger.debug("sub command recall")
        self.ide_inst.recall()

    def command_online_run_exec(self):
        self.logger.debug("sub command online_run")
        self.ide_inst.online_run()

    def command_status_exec(self):
        self.logger.debug("sub command status")
        self.ide_inst.status()
