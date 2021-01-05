import os
import subprocess

from .ph_ide_base import PhIDEBase
from phcli.ph_max_auto import define_value as dv
from phcli.ph_errs.ph_err import exception_file_not_exist
from phcli.ph_errs.ph_err import exception_function_not_implement
from phcli.ph_max_auto.ph_config.phconfig.phconfig import PhYAMLConfig


class PhIDEJupyter(PhIDEBase):
    """
    针对 Jupyter 环境的执行策略
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger.debug('maxauto PhIDEJupyter init')
        self.logger.debug(self.__dict__)

    def create(self, **kwargs):
        """
        jupyter的创建过程
        """
        self.logger.info('maxauto ide=jupyter 的 create 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.job_path)

        super().create()

    def run(self, **kwargs):
        """
        jupyter的运行过程
        """
        self.logger.info('maxauto ide=jupyter 的 run 实现')
        self.logger.debug(self.__dict__)
        self.logger.error('maxauto --ide=jupyter 时，不支持 run 子命令')

    def dag(self, **kwargs):
        """
        jupyter的DAG过程
        """
        self.logger.info('maxauto ide=jupyter 的 run 实现')
