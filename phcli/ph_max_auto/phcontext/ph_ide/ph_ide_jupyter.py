import os
import subprocess

from phcli.ph_errs.ph_err import *

from .ph_ide_base import PhIDEBase, logger, phs3


class PhIDEJupyter(PhIDEBase):
    """
    针对 Jupyter 环境的执行策略
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.__dict__.update(super().get_absolute_path())

    def create(self, **kwargs):
        """
        默认的创建过程
        """
        logger.info('maxauto 默认的 create 实现')

    def run(self, **kwargs):
        """
        默认的运行过程
        """
        logger.info('maxauto 默认的 run 实现')

    def combine(self, **kwargs):
        """
        默认的关联过程
        """
        logger.info('maxauto 默认的 combine 实现')

    def dag(self, **kwargs):
        """
        默认的DAG过程
        """
        logger.info('maxauto 默认的 dag 实现')

    def publish(self, **kwargs):
        """
        默认的发布过程
        """
        logger.info('maxauto 默认的 publish 实现')

    def submit(self, **kwargs):
        """
        默认的spark submit过程
        """
        logger.info('maxauto 默认的 submit 实现')

    def status(self, **kwargs):
        """
        默认的查看运行状态
        """
        logger.info('maxauto 默认的 status 实现')

