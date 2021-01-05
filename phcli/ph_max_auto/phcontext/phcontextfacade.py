# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
from phcli.ph_logs.ph_logs import phs3logger
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_c9 import PhIDEC9
from phcli.ph_max_auto.phcontext.ph_ide.ph_ide_jupyter import PhIDEJupyter


logger = phs3logger()
ide_table = {
    'c9': PhIDEC9,
    'jupyter': PhIDEJupyter,
}


class PhContextFacade(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def command_create_exec(self):
        logger.debug("sub command create")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.create()

    def command_run_exec(self):
        logger.debug("sub command run")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.run()

    def command_combine_exec(self):
        logger.debug("sub command combine")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.combine()

    def command_dag_exec(self):
        logger.debug("sub command dag")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.dag()

    def command_publish_exec(self):
        logger.debug("sub command publish")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.publish()

    def command_online_run_exec(self):
        logger.debug("sub command online_run")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.online_run()

    def command_status_exec(self):
        logger.debug("sub command status")
        ide_inst = ide_table[self.ide](**self.__dict__)
        ide_inst.status()
