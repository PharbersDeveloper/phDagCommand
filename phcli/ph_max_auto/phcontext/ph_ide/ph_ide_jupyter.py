from .ph_ide_base import PhIDEBase, PhCompleteStrategy


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
        jupyter 的创建过程
        """
        self.logger.info('maxauto ide=jupyter 的 create 实现')
        self.logger.debug(self.__dict__)

        self.check_path(self.job_path)

        super().create()

    def complete(self, **kwargs):
        """
        jupyter 的补全过程
        """
        self.logger.info('maxauto ide=jupyter 的 complete 实现')
        self.logger.debug(self.__dict__)
        cs = self.choice_complete_strategy(self.job_path + ".ipynb", self.job_path)

        if cs == PhCompleteStrategy.S2C:
            self.logger.info("S2C")
            runtime_inst = self.table_driver_runtime_inst(self.runtime)(**self.__dict__)
            runtime_inst.jupyter_to_c9(self.job_path + ".ipynb", self.job_path)
        elif cs == PhCompleteStrategy.C2S:
            self.logger.info("C2S")
            runtime_inst = self.table_driver_runtime_inst(self.runtime)(**self.__dict__)
            runtime_inst.c9_to_jupyter(self.job_path, self.job_path + ".ipynb")
        else:
            self.logger.info("KEEP COMPLETE")


